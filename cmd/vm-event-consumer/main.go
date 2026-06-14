package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type vmEvent struct {
	Type             string                   `json:"type"`
	Namespace        string                   `json:"namespace"`
	Name             string                   `json:"name"`
	UID              string                   `json:"uid"`
	ResourceVersion  string                   `json:"resourceVersion"`
	Generation       int64                    `json:"generation"`
	RunStrategy      string                   `json:"runStrategy,omitempty"`
	PrintableStatus  string                   `json:"status,omitempty"`
	EventFingerprint string                   `json:"eventFingerprint,omitempty"`
	Timestamp        time.Time                `json:"timestamp"`
	Labels           map[string]string        `json:"labels,omitempty"`
	Annotations      map[string]string        `json:"annotations,omitempty"`
	OwnerReferences  []map[string]interface{} `json:"ownerReferences,omitempty"`
	Spec             json.RawMessage          `json:"spec,omitempty"`
	StatusObject     json.RawMessage          `json:"statusObject,omitempty"`
	Disks            json.RawMessage          `json:"disks,omitempty"`
}

type sourceRow struct {
	ID               int64
	EventKey         string
	EventFingerprint string
	Payload          []byte
	CreatedAt        time.Time
}

type consumerConfig struct {
	DSN              string
	Name             string
	BatchSize        int
	PollInterval     time.Duration
	MaxTransitionGap time.Duration
	InitialBackoff   time.Duration
	MaxBackoff       time.Duration
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DSN)
	if err != nil {
		log.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := ensureSchema(ctx, pool); err != nil {
		log.Error("schema init", "err", err)
		os.Exit(1)
	}

	log.Info("consumer started",
		"name", cfg.Name,
		"batchSize", cfg.BatchSize,
		"pollInterval", cfg.PollInterval.String(),
		"maxTransitionGap", cfg.MaxTransitionGap.String())

	backoff := cfg.InitialBackoff
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("consumer stopped")
			return
		case <-ticker.C:
			processed, err := consumeBatch(ctx, pool, cfg, log)
			if err != nil {
				log.Error("consume batch", "err", err, "retryIn", backoff.String())
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				backoff *= 2
				if backoff > cfg.MaxBackoff {
					backoff = cfg.MaxBackoff
				}
				continue
			}
			backoff = cfg.InitialBackoff
			if processed > 0 {
				log.Info("batch processed", "rows", processed)
			}
		}
	}
}

func loadConfig() consumerConfig {
	return consumerConfig{
		DSN:              getenv("POSTGRES_DSN", "postgres://vmwatcher:changeme@postgres:5432/vmwatcher?sslmode=disable"),
		Name:             getenv("CONSUMER_NAME", "vm-event-consumer-1"),
		BatchSize:        getenvInt("BATCH_SIZE", 200),
		PollInterval:     getenvDuration("POLL_INTERVAL", 2*time.Second),
		MaxTransitionGap: getenvDuration("MAX_TRANSITION_GAP", 30*time.Minute),
		InitialBackoff:   getenvDuration("INITIAL_BACKOFF", 1*time.Second),
		MaxBackoff:       getenvDuration("MAX_BACKOFF", 30*time.Second),
	}
}

func ensureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS consumer_offsets (
			consumer_name TEXT PRIMARY KEY,
			last_event_id BIGINT NOT NULL DEFAULT 0,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS consumer_processed_events (
			consumer_name TEXT NOT NULL,
			event_fingerprint TEXT NOT NULL,
			event_id BIGINT NOT NULL,
			event_key TEXT NOT NULL,
			processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (consumer_name, event_fingerprint)
		)`,
		`CREATE TABLE IF NOT EXISTS vm_state (
			event_key TEXT PRIMARY KEY,
			namespace TEXT NOT NULL,
			name TEXT NOT NULL,
			last_event_id BIGINT NOT NULL,
			last_event_type TEXT NOT NULL,
			last_status TEXT,
			last_run_strategy TEXT,
			last_seen_at TIMESTAMPTZ NOT NULL,
			total_events BIGINT NOT NULL DEFAULT 0,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS vm_state_transitions (
			id BIGSERIAL PRIMARY KEY,
			consumer_name TEXT NOT NULL,
			event_key TEXT NOT NULL,
			from_status TEXT,
			to_status TEXT,
			event_type TEXT NOT NULL,
			event_id BIGINT NOT NULL,
			transition_at TIMESTAMPTZ NOT NULL,
			anomaly BOOLEAN NOT NULL DEFAULT false,
			reason TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_vm_state_transitions_event_key ON vm_state_transitions(event_key, transition_at DESC)`,
	}

	for _, q := range queries {
		if _, err := pool.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func consumeBatch(ctx context.Context, pool *pgxpool.Pool, cfg consumerConfig, log *slog.Logger) (int, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx,
		`INSERT INTO consumer_offsets(consumer_name, last_event_id)
		 VALUES ($1, 0)
		 ON CONFLICT (consumer_name) DO NOTHING`, cfg.Name); err != nil {
		return 0, err
	}

	var offset int64
	if err := tx.QueryRow(ctx,
		`SELECT last_event_id FROM consumer_offsets WHERE consumer_name=$1 FOR UPDATE`,
		cfg.Name).Scan(&offset); err != nil {
		return 0, err
	}

	rows, err := tx.Query(ctx,
		`SELECT id, event_key, event_fingerprint, payload, created_at
		 FROM vm_events
		 WHERE id > $1
		 ORDER BY id ASC
		 LIMIT $2`, offset, cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := make([]sourceRow, 0, cfg.BatchSize)
	for rows.Next() {
		var r sourceRow
		if err := rows.Scan(&r.ID, &r.EventKey, &r.EventFingerprint, &r.Payload, &r.CreatedAt); err != nil {
			return 0, err
		}
		batch = append(batch, r)
	}
	if rows.Err() != nil {
		return 0, rows.Err()
	}
	if len(batch) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return 0, err
		}
		return 0, nil
	}

	processed := 0
	lastID := offset
	for _, r := range batch {
		if err := processRow(ctx, tx, cfg, r, log); err != nil {
			return processed, err
		}
		lastID = r.ID
		processed++
	}

	if _, err := tx.Exec(ctx,
		`UPDATE consumer_offsets SET last_event_id=$2, updated_at=NOW() WHERE consumer_name=$1`,
		cfg.Name, lastID); err != nil {
		return processed, err
	}

	if err := tx.Commit(ctx); err != nil {
		return processed, err
	}
	return processed, nil
}

func processRow(ctx context.Context, tx pgx.Tx, cfg consumerConfig, r sourceRow, log *slog.Logger) error {
	var ev vmEvent
	if err := json.Unmarshal(r.Payload, &ev); err != nil {
		return fmt.Errorf("decode payload id=%d: %w", r.ID, err)
	}
	if strings.TrimSpace(ev.EventFingerprint) == "" {
		ev.EventFingerprint = r.EventFingerprint
	}
	if strings.TrimSpace(ev.EventFingerprint) == "" {
		return fmt.Errorf("missing event fingerprint id=%d", r.ID)
	}

	insertProcessed, err := tx.Exec(ctx,
		`INSERT INTO consumer_processed_events(consumer_name, event_fingerprint, event_id, event_key)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (consumer_name, event_fingerprint) DO NOTHING`,
		cfg.Name, ev.EventFingerprint, r.ID, r.EventKey)
	if err != nil {
		return err
	}
	if insertProcessed.RowsAffected() == 0 {
		log.Debug("duplicate skipped", "consumer", cfg.Name, "eventId", r.ID, "key", r.EventKey)
		return nil
	}

	ns, name := parseEventKey(r.EventKey, ev.Namespace, ev.Name)
	currStatus := strings.TrimSpace(ev.PrintableStatus)
	if currStatus == "" {
		currStatus = "UNKNOWN"
	}
	currType := strings.TrimSpace(ev.Type)
	if currType == "" {
		currType = "UNKNOWN"
	}

	var prevStatus string
	var prevSeen time.Time
	err = tx.QueryRow(ctx,
		`SELECT last_status, last_seen_at FROM vm_state WHERE event_key=$1`, r.EventKey).Scan(&prevStatus, &prevSeen)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	hasPrev := !errors.Is(err, pgx.ErrNoRows)

	if hasPrev && prevStatus != currStatus {
		anomaly, reason := detectTransitionAnomaly(prevStatus, currStatus, currType, prevSeen, r.CreatedAt, cfg.MaxTransitionGap)
		if _, err := tx.Exec(ctx,
			`INSERT INTO vm_state_transitions(consumer_name, event_key, from_status, to_status, event_type, event_id, transition_at, anomaly, reason)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			cfg.Name, r.EventKey, prevStatus, currStatus, currType, r.ID, r.CreatedAt, anomaly, reason); err != nil {
			return err
		}
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO vm_state(event_key, namespace, name, last_event_id, last_event_type, last_status, last_run_strategy, last_seen_at, total_events)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1)
		 ON CONFLICT (event_key)
		 DO UPDATE SET
			last_event_id = EXCLUDED.last_event_id,
			last_event_type = EXCLUDED.last_event_type,
			last_status = EXCLUDED.last_status,
			last_run_strategy = EXCLUDED.last_run_strategy,
			last_seen_at = EXCLUDED.last_seen_at,
			total_events = vm_state.total_events + 1,
			updated_at = NOW()`,
		r.EventKey, ns, name, r.ID, currType, currStatus, ev.RunStrategy, r.CreatedAt); err != nil {
		return err
	}

	return nil
}

func detectTransitionAnomaly(from, to, eventType string, prevSeen, currSeen time.Time, maxGap time.Duration) (bool, string) {
	if from == "" || to == "" || to == "UNKNOWN" {
		return false, ""
	}
	if eventType == "DELETED" {
		return false, ""
	}

	allowed := map[string]map[string]bool{
		"Stopped":  {"Starting": true, "Stopped": true},
		"Starting": {"Starting": true, "Running": true, "Stopped": true},
		"Running":  {"Running": true, "Stopping": true, "Stopped": true},
		"Stopping": {"Stopping": true, "Stopped": true, "Running": true},
	}
	if next, ok := allowed[from]; ok {
		if !next[to] {
			return true, "unexpected_status_transition"
		}
	}

	if !prevSeen.IsZero() && currSeen.Sub(prevSeen) > maxGap {
		return true, "large_transition_gap"
	}
	return false, ""
}

func parseEventKey(eventKey, fallbackNS, fallbackName string) (string, string) {
	parts := strings.SplitN(eventKey, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return fallbackNS, fallbackName
}

func getenv(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

func getenvInt(k string, def int) int {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

func getenvDuration(k string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return def
	}
	return d
}
