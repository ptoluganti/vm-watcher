package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type vmEvent struct {
	Type             string `json:"type"`
	Namespace        string `json:"namespace"`
	Name             string `json:"name"`
	EventFingerprint string `json:"eventFingerprint"`
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	dsn := getenv("POSTGRES_DSN", "")
	if strings.TrimSpace(dsn) == "" {
		log.Error("POSTGRES_DSN is required")
		os.Exit(1)
	}
	logDir := getenv("LOG_DIR", "/var/log/vm-watcher")
	podName := getenv("POD_NAME", "unknown-pod")
	publisherID := getenv("PUBLISHER_ID", podName)
	glob := strings.TrimSpace(getenv("EVENT_LOG_GLOB", ""))
	if glob == "" {
		glob = fmt.Sprintf("events-%s.jsonl*", podName)
	}
	pollInterval, err := time.ParseDuration(getenv("POLL_INTERVAL", "5s"))
	if err != nil || pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := ensureSchema(ctx, pool); err != nil {
		log.Error("schema init", "err", err)
		os.Exit(1)
	}

	log.Info("sidecar started", "logDir", logDir, "glob", glob, "publisherID", publisherID, "pollInterval", pollInterval.String())

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		inserted, skipped, err := publishFromFiles(ctx, pool, publisherID, logDir, glob)
		if err != nil {
			log.Error("publish cycle failed", "err", err)
		} else if inserted > 0 || skipped > 0 {
			log.Info("publish cycle", "inserted", inserted, "skippedDuplicates", skipped)
		}
		<-ticker.C
	}
}

func ensureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS vm_events (
			id BIGSERIAL PRIMARY KEY,
			event_key TEXT NOT NULL,
			event_fingerprint TEXT NOT NULL,
			payload JSONB NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `ALTER TABLE vm_events ADD COLUMN IF NOT EXISTS event_fingerprint TEXT`)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS ux_vm_events_fingerprint ON vm_events(event_fingerprint)`)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS vm_log_offsets (
			publisher_id TEXT NOT NULL,
			file_path TEXT NOT NULL,
			offset BIGINT NOT NULL DEFAULT 0,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (publisher_id, file_path)
		)`)
	return err
}

func publishFromFiles(ctx context.Context, pool *pgxpool.Pool, publisherID, logDir, pattern string) (int, int, error) {
	matches, err := filepath.Glob(filepath.Join(logDir, pattern))
	if err != nil {
		return 0, 0, err
	}
	sort.Strings(matches)
	inserted := 0
	skipped := 0
	for _, path := range matches {
		ins, skip, err := publishFile(ctx, pool, publisherID, path)
		if err != nil {
			return inserted, skipped, fmt.Errorf("%s: %w", path, err)
		}
		inserted += ins
		skipped += skip
	}
	return inserted, skipped, nil
}

func publishFile(ctx context.Context, pool *pgxpool.Pool, publisherID, path string) (int, int, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}

	offset, err := loadOffset(ctx, pool, publisherID, path)
	if err != nil {
		return 0, 0, err
	}
	if offset < 0 {
		offset = 0
	}
	if offset > info.Size() {
		// File was truncated/replaced.
		offset = 0
	}
	if _, err := f.Seek(offset, 0); err != nil {
		return 0, 0, err
	}

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 4*1024*1024)

	inserted := 0
	skipped := 0
	currentOffset := offset
	for scanner.Scan() {
		lineBytes := scanner.Bytes()
		currentOffset += int64(len(lineBytes)) + 1
		line := strings.TrimSpace(string(lineBytes))
		if line == "" {
			continue
		}
		var ev vmEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue
		}
		if strings.TrimSpace(ev.EventFingerprint) == "" {
			continue
		}
		key := fmt.Sprintf("%s/%s", ev.Namespace, ev.Name)
		cmd, err := pool.Exec(ctx,
			`INSERT INTO vm_events (event_key, event_fingerprint, payload)
			 VALUES ($1, $2, $3::jsonb)
			 ON CONFLICT (event_fingerprint) DO NOTHING`,
			key, ev.EventFingerprint, line)
		if err != nil {
			return inserted, skipped, err
		}
		if cmd.RowsAffected() == 1 {
			inserted++
		} else {
			skipped++
		}
	}
	if err := scanner.Err(); err != nil {
		return inserted, skipped, err
	}
	if err := storeOffset(ctx, pool, publisherID, path, currentOffset); err != nil {
		return inserted, skipped, err
	}
	return inserted, skipped, nil
}

func loadOffset(ctx context.Context, pool *pgxpool.Pool, publisherID, path string) (int64, error) {
	var offset int64
	err := pool.QueryRow(ctx,
		`SELECT offset FROM vm_log_offsets WHERE publisher_id=$1 AND file_path=$2`,
		publisherID, path).Scan(&offset)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return offset, nil
}

func storeOffset(ctx context.Context, pool *pgxpool.Pool, publisherID, path string, offset int64) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO vm_log_offsets (publisher_id, file_path, offset, updated_at)
		 VALUES ($1, $2, $3, NOW())
		 ON CONFLICT (publisher_id, file_path)
		 DO UPDATE SET offset=EXCLUDED.offset, updated_at=NOW()`,
		publisherID, path, offset)
	return err
}

func getenv(k, def string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	return v
}
