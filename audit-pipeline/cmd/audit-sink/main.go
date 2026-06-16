// Package main implements the vm-audit-sink: a Kafka consumer that reads
// kube-apiserver audit events from topic vm-audit-raw, filters for KubeVirt
// VirtualMachine mutations, and upserts them into PostgreSQL.
//
// WHY AUDIT LOG (not watch/informer)
// ────────────────────────────────────
// A client-go informer issues LIST+WATCH against the API server.  When the
// watch expires it re-lists and synthesises one synthetic ADD/UPDATE against
// the current state, silently collapsing every intermediate transition into
// that snapshot.  The kube-apiserver audit log records every mutating API
// call atomically and in order; it is the authoritative, lossless source for
// VM change history.
//
// WHY auditID IS THE DEDUP KEY
// ─────────────────────────────
// audit.k8s.io/v1.Event.AuditID is a per-request UUID assigned by the
// kube-apiserver.  It is stable across audit-log fanout, Vector collector
// restarts, Kafka at-least-once redelivery, and HA API-server replicas.
// Using it as PRIMARY KEY enables safe ON CONFLICT DO NOTHING upserts at any
// replay frequency.
//
// PERSIST-THEN-COMMIT
// ────────────────────
// The Kafka offset is committed ONLY after the row is confirmed written to
// PostgreSQL (CommitInterval: 0, manual CommitMessages).  If the process
// crashes after persist but before commit, the message is redelivered; the
// upsert is a harmless no-op, preserving exactly-once PostgreSQL semantics
// over an at-least-once Kafka delivery.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafka "github.com/segmentio/kafka-go"
)

// ── Minimal audit.k8s.io/v1 types ────────────────────────────────────────────
// Defined locally so the binary carries no k8s.io/apiserver dependency.

type auditEvent struct {
	// AuditID is the stable per-request UUID; used as the dedup/PK.
	AuditID                  string          `json:"auditID"`
	Stage                    string          `json:"stage"`
	Verb                     string          `json:"verb"`
	ObjectRef                *objectRef      `json:"objectRef,omitempty"`
	ResponseStatus           *responseStatus `json:"responseStatus,omitempty"`
	RequestReceivedTimestamp time.Time       `json:"requestReceivedTimestamp"`
	StageTimestamp           time.Time       `json:"stageTimestamp"`
	User                     userInfo        `json:"user"`
	RequestObject            json.RawMessage `json:"requestObject,omitempty"`
	ResponseObject           json.RawMessage `json:"responseObject,omitempty"`
}

type objectRef struct {
	Resource        string `json:"resource"`
	Namespace       string `json:"namespace"`
	Name            string `json:"name"`
	UID             string `json:"uid"`
	APIGroup        string `json:"apiGroup"`
	APIVersion      string `json:"apiVersion"`
	ResourceVersion string `json:"resourceVersion"`
	Subresource     string `json:"subresource,omitempty"`
}

type responseStatus struct {
	Code int32 `json:"code"`
}

type userInfo struct {
	Username string   `json:"username"`
	Groups   []string `json:"groups,omitempty"`
}

// logWrapper handles the OCP-Logging/Vector envelope that nests the raw audit
// event JSON as a string inside a top-level "message" field.
type logWrapper struct {
	Message string `json:"message"`
}

// ── Config ───────────────────────────────────────────────────────────────────

type config struct {
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string
	// KafkaMaxWait caps how long FetchMessage waits for new messages.
	// Low-volume environments benefit from the default 500ms (events arrive
	// within half a second rather than waiting for a full batch timeout).
	KafkaMaxWait time.Duration
	PostgresDSN  string
}

func loadConfig() config {
	maxWait, err := time.ParseDuration(getenv("KAFKA_MAX_WAIT", "500ms"))
	if err != nil || maxWait <= 0 {
		maxWait = 500 * time.Millisecond
	}
	return config{
		KafkaBrokers: strings.Split(getenv("KAFKA_BROKERS", "vm-audit-cluster-kafka-bootstrap:9092"), ","),
		KafkaTopic:   getenv("KAFKA_TOPIC", "vm-audit-raw"),
		KafkaGroupID: getenv("KAFKA_GROUP_ID", "vm-audit-sink"),
		KafkaMaxWait: maxWait,
		PostgresDSN:  getenv("POSTGRES_DSN", "postgres://audituser:changeme@postgres:5432/vmaudit?sslmode=disable"),
	}
}

// ── Schema ───────────────────────────────────────────────────────────────────
// The schema is also exported as schema.sql for the Makefile `schema` target.

const schemaSQL = `
CREATE TABLE IF NOT EXISTS vm_audit_events (
    -- auditID is the stable per-request UUID from the kube-apiserver; used
    -- as PRIMARY KEY so ON CONFLICT DO NOTHING gives idempotent upserts.
    audit_id              TEXT        PRIMARY KEY,
    verb                  TEXT        NOT NULL,
    stage                 TEXT        NOT NULL,
    namespace             TEXT,
    name                  TEXT,
    uid                   TEXT,
    resource_version      TEXT,
    response_code         INT,
    request_received_at   TIMESTAMPTZ NOT NULL,
    stage_timestamp       TIMESTAMPTZ NOT NULL,
    username              TEXT,
    request_object        JSONB,
    response_object       JSONB,
    -- raw stores the full canonical audit event JSON for forensic queries.
    raw                   JSONB       NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_vm_audit_ns_name
    ON vm_audit_events (namespace, name, stage_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_vm_audit_verb
    ON vm_audit_events (verb, stage_timestamp DESC);
`

const upsertSQL = `
INSERT INTO vm_audit_events (
    audit_id, verb, stage, namespace, name, uid, resource_version,
    response_code, request_received_at, stage_timestamp,
    username, request_object, response_object, raw
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
ON CONFLICT (audit_id) DO NOTHING`

// ── Main ─────────────────────────────────────────────────────────────────────

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		log.Error("ensure schema", "err", err)
		os.Exit(1)
	}
	log.Info("schema ready")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.KafkaTopic,
		GroupID:  cfg.KafkaGroupID,
		MinBytes: 1,                // return as soon as a single byte is available…
		MaxBytes: 10 * 1024 * 1024, // …but cap individual message size at 10 MB
		MaxWait:  cfg.KafkaMaxWait, // configurable; default 500ms keeps low-volume latency low
		// CommitInterval: 0 disables automatic offset commits.
		// Offsets are advanced manually via CommitMessages, only after a
		// successful PostgreSQL persist (see the persist-then-commit pattern above).
		CommitInterval: 0,
	})
	defer reader.Close()

	log.Info("audit-sink started",
		"brokers", cfg.KafkaBrokers,
		"topic", cfg.KafkaTopic,
		"group", cfg.KafkaGroupID,
		"maxWait", cfg.KafkaMaxWait)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Info("shutting down")
				return
			}
			log.Error("fetch message", "err", err)
			continue
		}

		// ── Cheap byte-level pre-filter ───────────────────────────────────
		// The audit log is extremely high-volume; most events concern pods,
		// nodes, secrets, etc.  A single Contains call eliminates them before
		// any JSON allocation, keeping CPU overhead negligible.
		if !bytes.Contains(msg.Value, []byte("virtualmachines")) {
			if err := reader.CommitMessages(ctx, msg); err != nil {
				log.Warn("commit (pre-filter skip)", "err", err)
			}
			continue
		}

		ev, raw, ok := parseAuditEvent(msg.Value, log)
		if !ok {
			// Malformed message: skip and commit to unblock the partition.
			if err := reader.CommitMessages(ctx, msg); err != nil {
				log.Warn("commit (malformed)", "err", err)
			}
			continue
		}

		if !wantEvent(ev) {
			if err := reader.CommitMessages(ctx, msg); err != nil {
				log.Warn("commit (filtered)", "err", err)
			}
			continue
		}

		// ── PERSIST first, COMMIT after ───────────────────────────────────
		// On crash between persist and commit the message is redelivered;
		// the ON CONFLICT DO NOTHING upsert is a safe, idempotent no-op.
		if err := persist(ctx, pool, ev, raw); err != nil {
			log.Error("persist failed – holding offset for redelivery",
				"auditID", ev.AuditID, "err", err)
			// Do not commit: the message will be redelivered after restart.
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			// Commit failure is non-fatal: the next deliver will re-upsert.
			log.Warn("commit failed after persist (idempotent redeliver expected)",
				"auditID", ev.AuditID, "err", err)
		}

		log.Info("stored",
			"auditID", ev.AuditID,
			"verb", ev.Verb,
			"namespace", nsOf(ev),
			"name", nameOf(ev),
			"code", codeOf(ev),
			"partition", msg.Partition,
			"offset", msg.Offset)
	}
}

// ── Parsing ──────────────────────────────────────────────────────────────────

// parseAuditEvent decodes a Kafka message value and returns the structured
// event plus the canonical JSON bytes to store in the raw column.
//
// Two wire formats are supported:
//  1. Raw audit-event JSON – emitted when Vector is configured with a
//     parse_json VRL transform or the `only_fields` sink option.
//  2. OCP-Logging structured envelope – {"message":"<escaped audit JSON>"}
//     produced by the default Fluentd/Vector collector configuration.
func parseAuditEvent(data []byte, log *slog.Logger) (auditEvent, []byte, bool) {
	var ev auditEvent
	if err := json.Unmarshal(data, &ev); err == nil && ev.AuditID != "" {
		return ev, data, true
	}

	// Fall back to OCP-Logging envelope format.
	var wrap logWrapper
	if err := json.Unmarshal(data, &wrap); err != nil || wrap.Message == "" {
		log.Debug("unrecognised message format", "snippet", snip(data))
		return auditEvent{}, nil, false
	}
	inner := []byte(wrap.Message)
	if err := json.Unmarshal(inner, &ev); err != nil || ev.AuditID == "" {
		log.Debug("inner message is not an audit event", "snippet", snip(inner))
		return auditEvent{}, nil, false
	}
	return ev, inner, true
}

// ── Filtering ────────────────────────────────────────────────────────────────

// wantEvent returns true for KubeVirt VirtualMachine mutations that completed
// successfully: stage ResponseComplete, apiGroup kubevirt.io, HTTP 2xx.
func wantEvent(ev auditEvent) bool {
	if ev.Stage != "ResponseComplete" {
		return false
	}
	if ev.ObjectRef == nil {
		return false
	}
	if ev.ObjectRef.Resource != "virtualmachines" {
		return false
	}
	if ev.ObjectRef.APIGroup != "kubevirt.io" {
		return false
	}
	code := codeOf(ev)
	return code >= 200 && code < 300
}

// ── Persistence ──────────────────────────────────────────────────────────────

// persist upserts one audit event into vm_audit_events.
func persist(ctx context.Context, pool *pgxpool.Pool, ev auditEvent, raw []byte) error {
	var ns, name, uid, rv string
	if ev.ObjectRef != nil {
		ns = ev.ObjectRef.Namespace
		name = ev.ObjectRef.Name
		uid = ev.ObjectRef.UID
		rv = ev.ObjectRef.ResourceVersion
	}

	requestedAt := ev.RequestReceivedTimestamp
	if requestedAt.IsZero() {
		requestedAt = time.Now().UTC()
	}
	stageAt := ev.StageTimestamp
	if stageAt.IsZero() {
		stageAt = time.Now().UTC()
	}

	_, err := pool.Exec(ctx, upsertSQL,
		ev.AuditID,
		ev.Verb,
		ev.Stage,
		ns, name, uid, rv,
		codeOf(ev),
		requestedAt,
		stageAt,
		ev.User.Username,
		nullJSON(ev.RequestObject),
		nullJSON(ev.ResponseObject),
		// Store raw as string; PostgreSQL coerces TEXT → JSONB on insert.
		string(raw),
	)
	return err
}

// nullJSON returns nil (→ SQL NULL) for empty or literal-null JSON, otherwise
// the raw bytes as a string that PostgreSQL will coerce to JSONB.
func nullJSON(b json.RawMessage) interface{} {
	if len(b) == 0 || bytes.Equal(b, []byte("null")) {
		return nil
	}
	return string(b)
}

// ── Accessors ────────────────────────────────────────────────────────────────

func codeOf(ev auditEvent) int32 {
	if ev.ResponseStatus == nil {
		return 0
	}
	return ev.ResponseStatus.Code
}

func nsOf(ev auditEvent) string {
	if ev.ObjectRef == nil {
		return ""
	}
	return ev.ObjectRef.Namespace
}

func nameOf(ev auditEvent) string {
	if ev.ObjectRef == nil {
		return ""
	}
	return ev.ObjectRef.Name
}

func snip(b []byte) string {
	if len(b) > 120 {
		return string(b[:120]) + "…"
	}
	return string(b)
}

func getenv(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}
