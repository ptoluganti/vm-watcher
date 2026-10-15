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
//
// micro-batches:
// Each batch is whatever has already been fetched (up to BATCH_MAX_SIZE).
// Its VM events are written in one round trip, then its offsets are queued
// for a background commit (KAFKA_COMMIT_INTERVAL). A lone event is written
// at once; under load the batches grow instead of a backlog.
//
// retry, never skip:
// Committing an offset also commits everything before it in the partition,
// so a failed write is retried until it succeeds. Only an event PostgreSQL
// rejects as invalid, which no retry can fix, goes to vm_audit_dead_letters.
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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	PostgresDSN         string
	KafkaBrokers        []string
	KafkaTopic          string
	KafkaGroupID        string
	KafkaMaxWait        time.Duration
	KafkaCommitInterval time.Duration
	BatchMaxSize        int
	RetryInitialBackoff time.Duration
	RetryMaxBackoff     time.Duration
	HTTPAddr            string
}

var (
	messagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "audit_sink_messages_total",
		Help: "Kafka messages handled, by result: stored, duplicate, filtered, malformed, dead_lettered.",
	}, []string{"result"})
	consumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "audit_sink_consumer_lag_messages",
		Help: "Messages behind the newest one in the partition, as of the last handled message.",
	}, []string{"partition"})
	eventLag = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "audit_sink_event_lag_seconds",
		Help:    "Time from the API server completing a request (stageTimestamp) to its row being stored.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300},
	})
	batchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "audit_sink_batch_size",
		Help:    "Kafka messages per batch.",
		Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
	})
	writeRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "audit_sink_write_retries_total",
		Help: "PostgreSQL writes that failed and were retried.",
	})
)

// messageReader is the part of *kafka.Reader the sink uses.
type messageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

// eventStore is the PostgreSQL side of the sink.
type eventStore interface {
	// insertEvents writes all rows or none, and reports per row whether it
	// was new (false means its auditID was already stored).
	insertEvents(ctx context.Context, rows []eventRow) ([]bool, error)
	deadLetter(ctx context.Context, msg kafka.Message, auditID string, cause error) error
	// createSchema creates whatever tables, columns and indexes are missing.
	createSchema(ctx context.Context) error
}

// eventRow is a wanted audit event and the Kafka message it came from.
type eventRow struct {
	msg kafka.Message
	ev  auditEvent
	raw []byte
}

type sink struct {
	log          *slog.Logger
	reader       messageReader
	store        eventStore
	batchMax     int
	retryInitial time.Duration
	retryMax     time.Duration
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
		CommitInterval: cfg.KafkaCommitInterval,
		// Background commits report failures only through this logger.
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			log.Error("kafka: " + fmt.Sprintf(msg, args...))
		}),
	})
	defer reader.Close() // also flushes pending offset commits

	server := &http.Server{Addr: cfg.HTTPAddr, Handler: httpMux()}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", "err", err)
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
		"commitInterval", cfg.KafkaCommitInterval.String(),
		"batchMaxSize", cfg.BatchMaxSize,
		"httpAddr", cfg.HTTPAddr)

	s := &sink{
		log:          log,
		reader:       reader,
		store:        pgStore{pool: pool},
		batchMax:     cfg.BatchMaxSize,
		retryInitial: cfg.RetryInitialBackoff,
		retryMax:     cfg.RetryMaxBackoff,
	}
	s.run(ctx)
	log.Info("shutting down")
}

// run handles messages batch by batch until ctx is done. Fetching happens in
// the background so the next batch collects while the current one is written.
func (s *sink) run(ctx context.Context) {
	msgs := make(chan kafka.Message, s.batchMax)
	go s.fetch(ctx, msgs)
	for {
		batch, ok := nextBatch(msgs, s.batchMax)
		if !ok || ctx.Err() != nil {
			return
		}
		if err := s.handleBatch(ctx, batch); err != nil {
			return
		}
	}
}

func (s *sink) fetch(ctx context.Context, out chan<- kafka.Message) {
	defer close(out)
	for {
		msg, err := s.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Error("fetch message", "err", err)
			if !sleepCtx(ctx, s.retryInitial) {
				return
			}
			continue
		}
		select {
		case out <- msg:
		case <-ctx.Done():
			return
		}
	}
}

// nextBatch waits for one message, then adds whatever is already waiting, up
// to limit. It never waits for a batch to fill, so a lone event is not
// delayed. ok is false once msgs is closed and empty.
func nextBatch(msgs <-chan kafka.Message, limit int) (batch []kafka.Message, ok bool) {
	msg, ok := <-msgs
	if !ok {
		return nil, false
	}
	batch = append(batch, msg)
	for len(batch) < limit {
		select {
		case msg, ok := <-msgs:
			if !ok {
				return batch, true
			}
			batch = append(batch, msg)
		default:
			return batch, true
		}
	}
	return batch, true
}

// handleBatch stores the VM events in batch, then commits every message in
// it. It returns an error only when ctx is done, and never commits past an
// event that is neither stored nor dead-lettered.
func (s *sink) handleBatch(ctx context.Context, batch []kafka.Message) error {
	batchSize.Observe(float64(len(batch)))

	var rows []eventRow
	for _, msg := range batch {
		// Cheap pre-filter: most audit traffic is not KubeVirt VM traffic.
		if !bytes.Contains(msg.Value, []byte("virtualmachines")) {
			messagesTotal.WithLabelValues("filtered").Inc()
			continue
		}
		ev, raw, ok := parseAuditEvent(msg.Value)
		if !ok {
			messagesTotal.WithLabelValues("malformed").Inc()
			s.log.Warn("skipping malformed audit message", "partition", msg.Partition, "offset", msg.Offset)
			continue
		}
		if !wantEvent(ev) {
			messagesTotal.WithLabelValues("filtered").Inc()
			continue
		}
		rows = append(rows, eventRow{msg: msg, ev: ev, raw: raw})
	}

	if len(rows) > 0 {
		if err := s.persist(ctx, rows); err != nil {
			return err
		}
	}

	if err := s.reader.CommitMessages(ctx, batch...); err != nil {
		if ctx.Err() != nil {
			return err
		}
		// A later commit covers these offsets too, and a redelivery in the
		// meantime is deduplicated by auditID.
		s.log.Warn("commit offsets", "err", err)
	}
	recordLag(batch)
	return nil
}

// persist writes rows, retrying outages until they succeed or ctx is done.
func (s *sink) persist(ctx context.Context, rows []eventRow) error {
	inserted, err := s.insert(ctx, rows)
	if err == nil {
		s.recordStored(rows, inserted)
		return nil
	}
	if !isRowError(err) {
		return err
	}
	// One rejected row rolls back the whole batch, so write the rows one at a
	// time and dead-letter only the ones PostgreSQL rejects.
	for _, row := range rows {
		one := []eventRow{row}
		inserted, err := s.insert(ctx, one)
		switch {
		case err == nil:
			s.recordStored(one, inserted)
		case isRowError(err):
			if err := s.deadLetter(ctx, row, err); err != nil {
				return err
			}
		default:
			return err
		}
	}
	return nil
}

func (s *sink) insert(ctx context.Context, rows []eventRow) ([]bool, error) {
	var inserted []bool
	err := s.retry(ctx, "insert audit events", func() (err error) {
		inserted, err = s.store.insertEvents(ctx, rows)
		s.repairSchema(ctx, err)
		return err
	}, isRowError)
	return inserted, err
}

func (s *sink) deadLetter(ctx context.Context, row eventRow, cause error) error {
	s.log.Error("PostgreSQL rejected audit event; moving it to vm_audit_dead_letters",
		"auditID", row.ev.AuditID,
		"partition", row.msg.Partition,
		"offset", row.msg.Offset,
		"err", cause)
	err := s.retry(ctx, "dead-letter audit event", func() error {
		err := s.store.deadLetter(ctx, row.msg, row.ev.AuditID, cause)
		s.repairSchema(ctx, err)
		return err
	}, nil)
	if err == nil {
		messagesTotal.WithLabelValues("dead_lettered").Inc()
	}
	return err
}

// repairSchema recreates the tables when err says they are missing, as when
// PostgreSQL comes back on a new, empty volume after the sink started. The
// failed write is then retried as usual; without the tables every retry would
// fail the same way.
func (s *sink) repairSchema(ctx context.Context, err error) {
	if !isMissingSchema(err) {
		return
	}
	s.log.Warn("PostgreSQL tables are missing; creating them", "err", err)
	if err := s.store.createSchema(ctx); err != nil {
		s.log.Error("create schema", "err", err)
	}
}

// retry runs op until it succeeds, ctx is done, or giveUp (when set) accepts
// its error. The wait doubles from retryInitial up to retryMax.
func (s *sink) retry(ctx context.Context, what string, op func() error, giveUp func(error) bool) error {
	delay := s.retryInitial
	for {
		err := op()
		if err == nil || (giveUp != nil && giveUp(err)) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		writeRetries.Inc()
		s.log.Error(what+" failed; retrying", "err", err, "retryIn", delay.String())
		if !sleepCtx(ctx, delay) {
			return ctx.Err()
		}
		delay = min(delay*2, s.retryMax)
	}
}

func (s *sink) recordStored(rows []eventRow, inserted []bool) {
	now := time.Now()
	for i, row := range rows {
		if i >= len(inserted) || !inserted[i] {
			messagesTotal.WithLabelValues("duplicate").Inc()
			continue
		}
		messagesTotal.WithLabelValues("stored").Inc()
		var lag time.Duration
		if ts := row.ev.StageTimestamp; !ts.IsZero() {
			lag = now.Sub(ts)
			eventLag.Observe(lag.Seconds())
		}
		ref := row.ev.ObjectRef
		s.log.Info("stored audit event",
			"auditID", row.ev.AuditID,
			"verb", row.ev.Verb,
			"namespace", ref.Namespace,
			"name", ref.Name,
			"subresource", ref.Subresource,
			"code", responseCodeOf(row.ev),
			"partition", row.msg.Partition,
			"offset", row.msg.Offset,
			"lag", lag.String())
	}
}

// recordLag sets, per partition, how far the last handled message was behind
// the newest message in that partition when it was fetched.
func recordLag(batch []kafka.Message) {
	last := make(map[int]kafka.Message)
	for _, msg := range batch {
		last[msg.Partition] = msg
	}
	for partition, msg := range last {
		consumerLag.WithLabelValues(strconv.Itoa(partition)).Set(float64(partitionLag(msg)))
	}
}

func partitionLag(msg kafka.Message) int64 {
	return max(msg.HighWaterMark-msg.Offset-1, 0)
}

// isRowError reports whether PostgreSQL rejected the data itself (SQLSTATE
// class 22 data exception, 23 integrity violation, 54 program limit), which
// no retry can fix. Anything else, such as a lost connection, a timeout, a
// full disk, or a missing permission, is an outage and is retried.
func isRowError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || len(pgErr.Code) < 2 {
		return false
	}
	switch pgErr.Code[:2] {
	case "22", "23", "54":
		return true
	}
	return false
}

// isMissingSchema reports whether PostgreSQL says a table (SQLSTATE 42P01) or
// column (42703) does not exist.
func isMissingSchema(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "42P01" || pgErr.Code == "42703")
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func loadConfig() (config, error) {
	cfg := config{
		KafkaTopic:   getenv("KAFKA_TOPIC", "vm-audit-raw"),
		KafkaGroupID: getenv("KAFKA_GROUP_ID", "vm-audit-sink"),
		HTTPAddr:     getenv("HTTP_ADDR", ":8080"),
	}
	var err error
	if cfg.PostgresDSN, err = requiredEnv("POSTGRES_DSN"); err != nil {
		return config{}, err
	}
	if cfg.KafkaBrokers, err = csvEnv("KAFKA_BROKERS"); err != nil {
		return config{}, err
	}
	if cfg.KafkaMaxWait, err = durationEnv("KAFKA_MAX_WAIT", 500*time.Millisecond); err != nil {
		return config{}, err
	}
	if cfg.KafkaCommitInterval, err = durationEnv("KAFKA_COMMIT_INTERVAL", time.Second); err != nil {
		return config{}, err
	}
	if cfg.BatchMaxSize, err = intEnv("BATCH_MAX_SIZE", 500); err != nil {
		return config{}, err
	}
	if cfg.RetryInitialBackoff, err = durationEnv("RETRY_INITIAL_BACKOFF", 200*time.Millisecond); err != nil {
		return config{}, err
	}
	if cfg.RetryMaxBackoff, err = durationEnv("RETRY_MAX_BACKOFF", 10*time.Second); err != nil {
		return config{}, err
	}

	switch {
	case cfg.KafkaMaxWait <= 0:
		return config{}, errors.New("KAFKA_MAX_WAIT must be positive")
	case cfg.KafkaCommitInterval < 0:
		return config{}, errors.New("KAFKA_COMMIT_INTERVAL must not be negative (0 commits synchronously)")
	case cfg.BatchMaxSize < 1:
		return config{}, errors.New("BATCH_MAX_SIZE must be at least 1")
	case cfg.RetryInitialBackoff <= 0 || cfg.RetryMaxBackoff < cfg.RetryInitialBackoff:
		return config{}, errors.New("RETRY_INITIAL_BACKOFF must be positive and at most RETRY_MAX_BACKOFF")
	}
	return cfg, nil
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

func durationEnv(k string, def time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", k, err)
	}
	return d, nil
}

func intEnv(k string, def int) (int, error) {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", k, err)
	}
	return n, nil
}

func ensureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	for _, stmt := range []string{`
CREATE TABLE IF NOT EXISTS vm_audit_events (
    audit_id            TEXT PRIMARY KEY,
    verb                TEXT NOT NULL,
    stage               TEXT NOT NULL,
    api_group           TEXT,
    namespace           TEXT,
    name                TEXT,
    subresource         TEXT,
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
)`,
		// Columns added after the first release.
		`ALTER TABLE vm_audit_events ADD COLUMN IF NOT EXISTS api_group TEXT`,
		`ALTER TABLE vm_audit_events ADD COLUMN IF NOT EXISTS subresource TEXT`,
		`CREATE INDEX IF NOT EXISTS idx_vm_audit_ns_name ON vm_audit_events(namespace, name, stage_timestamp DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_vm_audit_verb ON vm_audit_events(verb, stage_timestamp DESC)`,
		// payload is BYTEA because a rejected event may not be valid text or JSON.
		`
CREATE TABLE IF NOT EXISTS vm_audit_dead_letters (
    kafka_topic     TEXT NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset    BIGINT NOT NULL,
    audit_id        TEXT,
    error           TEXT NOT NULL,
    payload         BYTEA NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
)`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
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

// mutatingVerbs are the audit verbs that change a VirtualMachine.
var mutatingVerbs = map[string]bool{
	"create": true, "update": true, "patch": true, "delete": true, "deletecollection": true,
}

func wantEvent(ev auditEvent) bool {
	if ev.Stage != "ResponseComplete" || ev.ObjectRef == nil || !mutatingVerbs[ev.Verb] {
		return false
	}
	if ev.ObjectRef.Resource != "virtualmachines" {
		return false
	}
	// subresources.kubevirt.io carries start/stop/restart/migrate requests,
	// the only audit events that name the user behind those actions.
	if g := ev.ObjectRef.APIGroup; g != "kubevirt.io" && g != "subresources.kubevirt.io" {
		return false
	}
	code := responseCodeOf(ev)
	return code >= 200 && code < 300
}

type pgStore struct {
	pool *pgxpool.Pool
}

func (p pgStore) createSchema(ctx context.Context) error {
	return ensureSchema(ctx, p.pool)
}

const insertEventSQL = `
INSERT INTO vm_audit_events (
    audit_id, verb, stage, api_group, namespace, name, subresource, uid,
    resource_version, response_code, request_received_at, stage_timestamp,
    username, request_object, response_object, raw
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
ON CONFLICT (audit_id) DO NOTHING
`

func (p pgStore) insertEvents(ctx context.Context, rows []eventRow) ([]bool, error) {
	batch := &pgx.Batch{}
	for _, row := range rows {
		batch.Queue(insertEventSQL, row.insertArgs()...)
	}
	// pgx runs a batch as one implicit transaction: all rows or none.
	results := p.pool.SendBatch(ctx, batch)
	inserted := make([]bool, len(rows))
	for i := range rows {
		tag, err := results.Exec()
		if err != nil {
			_ = results.Close()
			return nil, err
		}
		inserted[i] = tag.RowsAffected() == 1
	}
	return inserted, results.Close()
}

func (p pgStore) deadLetter(ctx context.Context, msg kafka.Message, auditID string, cause error) error {
	_, err := p.pool.Exec(ctx, `
INSERT INTO vm_audit_dead_letters (kafka_topic, kafka_partition, kafka_offset, audit_id, error, payload)
VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
`,
		msg.Topic,
		msg.Partition,
		msg.Offset,
		cleanText(auditID),
		cleanText(cause.Error()),
		msg.Value,
	)
	return err
}

func (r eventRow) insertArgs() []any {
	ev := r.ev
	var apiGroup, namespace, name, subresource, uid, resourceVersion string
	if ev.ObjectRef != nil {
		apiGroup = ev.ObjectRef.APIGroup
		namespace = ev.ObjectRef.Namespace
		name = ev.ObjectRef.Name
		subresource = ev.ObjectRef.Subresource
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

	return []any{
		ev.AuditID,
		ev.Verb,
		ev.Stage,
		apiGroup,
		namespace,
		name,
		subresource,
		uid,
		resourceVersion,
		responseCodeOf(ev),
		requestReceivedAt,
		stageTimestamp,
		ev.User.Username,
		jsonValue(ev.RequestObject),
		jsonValue(ev.ResponseObject),
		string(r.raw),
	}
}

func jsonValue(b json.RawMessage) any {
	if len(b) == 0 || bytes.Equal(bytes.TrimSpace(b), []byte("null")) {
		return nil
	}
	return string(b)
}

// cleanText makes s fit a TEXT column, which rejects NUL bytes and invalid UTF-8.
func cleanText(s string) string {
	return strings.ToValidUTF8(strings.ReplaceAll(s, "\x00", ""), "�")
}

func responseCodeOf(ev auditEvent) int32 {
	if ev.ResponseStatus == nil {
		return 0
	}
	return ev.ResponseStatus.Code
}

func httpMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

func getenv(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}
