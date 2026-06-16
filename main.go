// vm-audit-sink consumes kube-apiserver audit events for KubeVirt
// VirtualMachine mutations from Kafka and stores them in PostgreSQL.
//
// Audit log, not watch/informer:
// A watch re-lists on expiry and can collapse intermediate transitions.
// The kube-apiserver audit log is authoritative: it records every mutating
// request, which we stream through Vector/ClusterLogForwarder into Kafka.
//
// auditID as dedupe key:
// audit.k8s.io/v1.Event.AuditID is a stable per-request UUID. Using it as
// the PRIMARY KEY makes Kafka redelivery, collector restarts, and retries
// idempotent without silent loss.
//
// persist-then-commit:
// The Kafka offset is committed only after PostgreSQL confirms the row.
// If the process crashes in between, the message is redelivered and the
// ON CONFLICT DO NOTHING upsert is a harmless no-op.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafka "github.com/segmentio/kafka-go"
)

type auditEvent struct {
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

// Vector can emit the raw audit event directly or wrap it inside a top-level
// message field depending on the collector configuration.
type logEnvelope struct {
	Message string `json:"message"`
}

type config struct {
	PostgresDSN  string
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string
	KafkaMaxWait time.Duration
	HTTPAddr     string
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := loadConfig()
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := ensureSchema(ctx, pool); err != nil {
		log.Error("ensure schema", "err", err)
		os.Exit(1)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		Topic:          cfg.KafkaTopic,
		GroupID:        cfg.KafkaGroupID,
		MinBytes:       1,
		MaxBytes:       10 * 1024 * 1024,
		MaxWait:        cfg.KafkaMaxWait,
		CommitInterval: 0,
	})
	defer reader.Close()

	server := &http.Server{Addr: cfg.HTTPAddr, Handler: healthMux()}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server", "err", err)
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Info("audit sink started",
		"brokers", strings.Join(cfg.KafkaBrokers, ","),
		"topic", cfg.KafkaTopic,
		"groupID", cfg.KafkaGroupID,
		"maxWait", cfg.KafkaMaxWait.String(),
		"httpAddr", cfg.HTTPAddr)

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

		// Cheap pre-filter: most audit traffic is not KubeVirt VM traffic.
		if !bytes.Contains(msg.Value, []byte("virtualmachines")) {
			if err := reader.CommitMessages(ctx, msg); err != nil {
				log.Warn("commit after pre-filter skip", "err", err)
			}
			continue
		}

		ev, raw, ok := parseAuditEvent(msg.Value)
		if !ok {
			if err := reader.CommitMessages(ctx, msg); err != nil {
				log.Warn("commit after malformed message", "err", err)
			}
			continue
		}

		if !wantEvent(ev) {
			if err := reader.CommitMessages(ctx, msg); err != nil {
				log.Warn("commit after filtered message", "err", err)
			}
			continue
		}

		if err := persist(ctx, pool, ev, raw); err != nil {
			log.Error("persist failed; leaving offset uncommitted for redelivery", "auditID", ev.AuditID, "err", err)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Warn("commit failed after persist", "auditID", ev.AuditID, "err", err)
		}

		log.Info("stored audit event",
			"auditID", ev.AuditID,
			"verb", ev.Verb,
			"namespace", namespaceOf(ev),
			"name", nameOf(ev),
			"code", responseCodeOf(ev),
			"partition", msg.Partition,
			"offset", msg.Offset)
	}
}

func loadConfig() (config, error) {
	postgresDSN, err := requiredEnv("POSTGRES_DSN")
	if err != nil {
		return config{}, err
	}
	brokers, err := csvEnv("KAFKA_BROKERS")
	if err != nil {
		return config{}, err
	}
	maxWait, err := time.ParseDuration(getenv("KAFKA_MAX_WAIT", "500ms"))
	if err != nil || maxWait <= 0 {
		maxWait = 500 * time.Millisecond
	}
	return config{
		PostgresDSN:  postgresDSN,
		KafkaBrokers: brokers,
		KafkaTopic:   getenv("KAFKA_TOPIC", "vm-audit-raw"),
		KafkaGroupID: getenv("KAFKA_GROUP_ID", "vm-audit-sink"),
		KafkaMaxWait: maxWait,
		HTTPAddr:     getenv("HTTP_ADDR", ":8080"),
	}, nil
}

func requiredEnv(k string) (string, error) {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return "", fmt.Errorf("%s is required", k)
	}
	return v, nil
}

func csvEnv(k string) ([]string, error) {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return nil, fmt.Errorf("%s is required", k)
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s is required", k)
	}
	return out, nil
}

func ensureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS vm_audit_events (
    audit_id            TEXT PRIMARY KEY,
    verb                TEXT NOT NULL,
    stage               TEXT NOT NULL,
    namespace           TEXT,
    name                TEXT,
    uid                 TEXT,
    resource_version    TEXT,
    response_code       INT,
    request_received_at TIMESTAMPTZ NOT NULL,
    stage_timestamp     TIMESTAMPTZ NOT NULL,
    username            TEXT,
    request_object      JSONB,
    response_object     JSONB,
    raw                 JSONB NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
`)
	if err != nil {
		return err
	}
	if _, err = pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_vm_audit_ns_name ON vm_audit_events(namespace, name, stage_timestamp DESC)`); err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_vm_audit_verb ON vm_audit_events(verb, stage_timestamp DESC)`)
	return err
}

func parseAuditEvent(data []byte) (auditEvent, []byte, bool) {
	var ev auditEvent
	if err := json.Unmarshal(data, &ev); err == nil && ev.AuditID != "" {
		return ev, data, true
	}

	var envelope logEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil || envelope.Message == "" {
		return auditEvent{}, nil, false
	}
	inner := []byte(envelope.Message)
	if err := json.Unmarshal(inner, &ev); err != nil || ev.AuditID == "" {
		return auditEvent{}, nil, false
	}
	return ev, inner, true
}

func wantEvent(ev auditEvent) bool {
	if ev.Stage != "ResponseComplete" || ev.ObjectRef == nil {
		return false
	}
	if ev.ObjectRef.Resource != "virtualmachines" || ev.ObjectRef.APIGroup != "kubevirt.io" {
		return false
	}
	code := responseCodeOf(ev)
	return code >= 200 && code < 300
}

func persist(ctx context.Context, pool *pgxpool.Pool, ev auditEvent, raw []byte) error {
	var namespace, name, uid, resourceVersion string
	if ev.ObjectRef != nil {
		namespace = ev.ObjectRef.Namespace
		name = ev.ObjectRef.Name
		uid = ev.ObjectRef.UID
		resourceVersion = ev.ObjectRef.ResourceVersion
	}

	requestReceivedAt := ev.RequestReceivedTimestamp
	if requestReceivedAt.IsZero() {
		requestReceivedAt = time.Now().UTC()
	}
	stageTimestamp := ev.StageTimestamp
	if stageTimestamp.IsZero() {
		stageTimestamp = time.Now().UTC()
	}

	_, err := pool.Exec(ctx, `
INSERT INTO vm_audit_events (
    audit_id, verb, stage, namespace, name, uid, resource_version,
    response_code, request_received_at, stage_timestamp,
    username, request_object, response_object, raw
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
ON CONFLICT (audit_id) DO NOTHING
`,
		ev.AuditID,
		ev.Verb,
		ev.Stage,
		namespace,
		name,
		uid,
		resourceVersion,
		responseCodeOf(ev),
		requestReceivedAt,
		stageTimestamp,
		ev.User.Username,
		jsonValue(ev.RequestObject),
		jsonValue(ev.ResponseObject),
		string(raw),
	)
	return err
}

func jsonValue(b json.RawMessage) any {
	if len(b) == 0 || bytes.Equal(bytes.TrimSpace(b), []byte("null")) {
		return nil
	}
	return string(b)
}

func responseCodeOf(ev auditEvent) int32 {
	if ev.ResponseStatus == nil {
		return 0
	}
	return ev.ResponseStatus.Code
}

func namespaceOf(ev auditEvent) string {
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

func healthMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

func getenv(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}
