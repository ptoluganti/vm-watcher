// vm-event-consumer stores the live VM state lane in PostgreSQL. It reads the
// VirtualMachine changes that cmd/vm-watcher publishes to Kafka topic
// vm-events and keeps vm_state (the latest state of each VM) and
// vm_state_transitions (status changes, with anomaly flags).
//
// persist-then-commit:
// Offsets are committed only after PostgreSQL has the batch, in the
// background every KAFKA_COMMIT_INTERVAL. A replay after a crash is
// deduplicated by eventFingerprint in consumer_processed_events.
//
// micro-batches:
// Each batch is whatever has already been fetched (up to BATCH_MAX_SIZE) and
// is applied in one transaction of a few round trips. A lone event is written
// at once; under load the batches grow instead of a backlog.
//
// retry, never skip:
// Committing an offset also commits everything before it in the partition,
// so a failed write is retried until it succeeds. Only a message that is not
// a usable VM event, or one PostgreSQL rejects as invalid, goes to
// vm_event_dead_letters.
//
// newest wins:
// resourceVersion orders the changes to one VM. An event older than the
// stored state (a watcher retry or rollout can deliver one late) is recorded
// as processed but does not move vm_state backwards.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
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

type config struct {
	PostgresDSN         string
	ConsumerName        string
	KafkaBrokers        []string
	KafkaTopic          string
	KafkaGroupID        string
	KafkaMaxWait        time.Duration
	KafkaCommitInterval time.Duration
	BatchMaxSize        int
	MaxTransitionGap    time.Duration
	RetryInitialBackoff time.Duration
	RetryMaxBackoff     time.Duration
	HTTPAddr            string
}

var (
	messagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vm_event_consumer_messages_total",
		Help: "Kafka messages handled, by result: applied, stale, duplicate, dead_lettered.",
	}, []string{"result"})
	consumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vm_event_consumer_lag_messages",
		Help: "Messages behind the newest one in the partition, as of the last handled message.",
	}, []string{"partition"})
	eventLag = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "vm_event_consumer_event_lag_seconds",
		Help:    "Time from the watcher observing a VM change to its state being stored.",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30},
	})
	batchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "vm_event_consumer_batch_size",
		Help:    "Kafka messages per batch.",
		Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
	})
	writeRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vm_event_consumer_write_retries_total",
		Help: "PostgreSQL writes that failed and were retried.",
	})
)

// messageReader is the part of *kafka.Reader the consumer uses.
type messageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

// stateStore is the PostgreSQL side of the consumer.
type stateStore interface {
	// apply records rows, in order, in one transaction: all of them or none.
	// It reports what each row did.
	apply(ctx context.Context, rows []eventRow) ([]outcome, error)
	deadLetter(ctx context.Context, msg kafka.Message, cause error) error
}

// eventRow is a decoded VM event and the Kafka message it came from.
type eventRow struct {
	msg kafka.Message
	key string // namespace/name
	ev  vmEvent
}

// outcome is what applying one event did.
type outcome int

const (
	applied   outcome = iota // vm_state now reflects it
	stale                    // older than the stored state, so only recorded
	duplicate                // already processed
)

type consumer struct {
	log          *slog.Logger
	reader       messageReader
	store        stateStore
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
		log.Error("schema init", "err", err)
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

	log.Info("vm event consumer started",
		"brokers", strings.Join(cfg.KafkaBrokers, ","),
		"topic", cfg.KafkaTopic,
		"groupID", cfg.KafkaGroupID,
		"consumerName", cfg.ConsumerName,
		"commitInterval", cfg.KafkaCommitInterval.String(),
		"batchMaxSize", cfg.BatchMaxSize,
		"httpAddr", cfg.HTTPAddr)

	c := &consumer{
		log:          log,
		reader:       reader,
		store:        pgStore{pool: pool, consumer: cfg.ConsumerName, maxGap: cfg.MaxTransitionGap},
		batchMax:     cfg.BatchMaxSize,
		retryInitial: cfg.RetryInitialBackoff,
		retryMax:     cfg.RetryMaxBackoff,
	}
	c.run(ctx)
	log.Info("shutting down")
}

// run handles messages batch by batch until ctx is done. Fetching happens in
// the background so the next batch collects while the current one is written.
func (c *consumer) run(ctx context.Context) {
	msgs := make(chan kafka.Message, c.batchMax)
	go c.fetch(ctx, msgs)
	for {
		batch, ok := nextBatch(msgs, c.batchMax)
		if !ok || ctx.Err() != nil {
			return
		}
		if err := c.handleBatch(ctx, batch); err != nil {
			return
		}
	}
}

func (c *consumer) fetch(ctx context.Context, out chan<- kafka.Message) {
	defer close(out)
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.Error("fetch message", "err", err)
			if !sleepCtx(ctx, c.retryInitial) {
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

// handleBatch applies the VM events in batch, then commits every message in
// it. It returns an error only when ctx is done, and never commits past an
// event that is neither applied nor dead-lettered.
func (c *consumer) handleBatch(ctx context.Context, batch []kafka.Message) error {
	batchSize.Observe(float64(len(batch)))

	var rows []eventRow
	for _, msg := range batch {
		row, err := decodeEvent(msg)
		if err != nil {
			if err := c.deadLetter(ctx, msg, err); err != nil {
				return err
			}
			continue
		}
		rows = append(rows, row)
	}

	if len(rows) > 0 {
		if err := c.persist(ctx, rows); err != nil {
			return err
		}
	}

	if err := c.reader.CommitMessages(ctx, batch...); err != nil {
		if ctx.Err() != nil {
			return err
		}
		// A later commit covers these offsets too, and a redelivery in the
		// meantime is deduplicated by eventFingerprint.
		c.log.Warn("commit offsets", "err", err)
	}
	recordLag(batch)
	return nil
}

// decodeEvent parses msg. An error means no retry can ever apply it.
func decodeEvent(msg kafka.Message) (eventRow, error) {
	var ev vmEvent
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		return eventRow{}, fmt.Errorf("decode VM event: %w", err)
	}
	if strings.TrimSpace(ev.EventFingerprint) == "" {
		return eventRow{}, errors.New("VM event has no eventFingerprint")
	}
	key := strings.TrimSpace(string(msg.Key))
	if key == "" {
		key = ev.Namespace + "/" + ev.Name
	}
	if ns, name := parseEventKey(key, ev.Namespace, ev.Name); ns == "" || name == "" {
		return eventRow{}, fmt.Errorf("VM event has no namespace/name (key %q)", key)
	}
	return eventRow{msg: msg, key: key, ev: ev}, nil
}

// persist applies rows, retrying outages until they succeed or ctx is done.
func (c *consumer) persist(ctx context.Context, rows []eventRow) error {
	outcomes, err := c.apply(ctx, rows)
	if err == nil {
		c.recordApplied(rows, outcomes)
		return nil
	}
	if !isRowError(err) {
		return err
	}
	// One rejected row rolls back the whole batch, so apply the rows one at a
	// time and dead-letter only the ones PostgreSQL rejects.
	for _, row := range rows {
		one := []eventRow{row}
		outcomes, err := c.apply(ctx, one)
		switch {
		case err == nil:
			c.recordApplied(one, outcomes)
		case isRowError(err):
			if err := c.deadLetter(ctx, row.msg, err); err != nil {
				return err
			}
		default:
			return err
		}
	}
	return nil
}

func (c *consumer) apply(ctx context.Context, rows []eventRow) ([]outcome, error) {
	var outcomes []outcome
	err := c.retry(ctx, "apply VM events", func() (err error) {
		outcomes, err = c.store.apply(ctx, rows)
		return err
	}, isRowError)
	return outcomes, err
}

func (c *consumer) deadLetter(ctx context.Context, msg kafka.Message, cause error) error {
	c.log.Error("cannot apply VM event; moving it to vm_event_dead_letters",
		"key", string(msg.Key),
		"partition", msg.Partition,
		"offset", msg.Offset,
		"err", cause)
	err := c.retry(ctx, "dead-letter VM event", func() error {
		return c.store.deadLetter(ctx, msg, cause)
	}, nil)
	if err == nil {
		messagesTotal.WithLabelValues("dead_lettered").Inc()
	}
	return err
}

// retry runs op until it succeeds, ctx is done, or giveUp (when set) accepts
// its error. The wait doubles from retryInitial up to retryMax.
func (c *consumer) retry(ctx context.Context, what string, op func() error, giveUp func(error) bool) error {
	delay := c.retryInitial
	for {
		err := op()
		if err == nil || (giveUp != nil && giveUp(err)) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		writeRetries.Inc()
		c.log.Error(what+" failed; retrying", "err", err, "retryIn", delay.String())
		if !sleepCtx(ctx, delay) {
			return ctx.Err()
		}
		delay = min(delay*2, c.retryMax)
	}
}

func (c *consumer) recordApplied(rows []eventRow, outcomes []outcome) {
	now := time.Now()
	for i, row := range rows {
		if i >= len(outcomes) {
			break
		}
		switch outcomes[i] {
		case duplicate:
			messagesTotal.WithLabelValues("duplicate").Inc()
		case stale:
			messagesTotal.WithLabelValues("stale").Inc()
			c.log.Info("VM event older than the stored state; recorded without changing vm_state",
				"key", row.key,
				"type", row.ev.Type,
				"resourceVersion", row.ev.ResourceVersion,
				"partition", row.msg.Partition,
				"offset", row.msg.Offset)
		default:
			messagesTotal.WithLabelValues("applied").Inc()
			var lag time.Duration
			if at := row.observedAt(); !at.IsZero() {
				lag = now.Sub(at)
				eventLag.Observe(lag.Seconds())
			}
			c.log.Info("applied vm event",
				"key", row.key,
				"type", row.ev.Type,
				"status", row.ev.PrintableStatus,
				"resourceVersion", row.ev.ResourceVersion,
				"partition", row.msg.Partition,
				"offset", row.msg.Offset,
				"lag", lag.String())
		}
	}
}

// observedAt is when the watcher saw the change.
func (r eventRow) observedAt() time.Time {
	if !r.ev.Timestamp.IsZero() {
		return r.ev.Timestamp
	}
	return r.msg.Time
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

// vmState is the part of a vm_state row that orders and classifies changes.
type vmState struct {
	status          string
	seenAt          time.Time
	resourceVersion string
}

// transition is a vm_state_transitions row.
type transition struct {
	key             string
	from, to        string
	eventType       string
	eventID         int64
	resourceVersion string
	at              time.Time
	anomaly         bool
	reason          string
}

// stateUpdate is the vm_state row of one VM after a batch.
type stateUpdate struct {
	key, namespace, name string
	eventID              int64
	eventType            string
	status               string
	runStrategy          string
	resourceVersion      string
	seenAt               time.Time
	events               int64 // events applied in this batch
}

// plan works out, in order, what rows do to the VM states in states, and
// updates states to match. isNew reports which rows were not processed before.
func plan(states map[string]vmState, rows []eventRow, isNew []bool, maxGap time.Duration) ([]outcome, []transition, []stateUpdate) {
	outcomes := make([]outcome, len(rows))
	var transitions []transition
	updates := make(map[string]*stateUpdate)
	for i, row := range rows {
		if !isNew[i] {
			outcomes[i] = duplicate
			continue
		}
		prev, known := states[row.key]
		if known && olderVersion(row.ev.ResourceVersion, prev.resourceVersion) {
			outcomes[i] = stale
			continue
		}

		status, eventType, at := orUnknown(row.ev.PrintableStatus), orUnknown(row.ev.Type), row.observedAt()
		if known && prev.status != status {
			anomaly, reason := detectTransitionAnomaly(prev.status, status, eventType, prev.seenAt, at, maxGap)
			transitions = append(transitions, transition{
				key:             row.key,
				from:            prev.status,
				to:              status,
				eventType:       eventType,
				eventID:         row.msg.Offset,
				resourceVersion: row.ev.ResourceVersion,
				at:              at,
				anomaly:         anomaly,
				reason:          reason,
			})
		}
		states[row.key] = vmState{status: status, seenAt: at, resourceVersion: row.ev.ResourceVersion}

		u := updates[row.key]
		if u == nil {
			ns, name := parseEventKey(row.key, row.ev.Namespace, row.ev.Name)
			u = &stateUpdate{key: row.key, namespace: ns, name: name}
			updates[row.key] = u
		}
		u.eventID = row.msg.Offset
		u.eventType = eventType
		u.status = status
		u.runStrategy = row.ev.RunStrategy
		u.resourceVersion = row.ev.ResourceVersion
		u.seenAt = at
		u.events++
		outcomes[i] = applied
	}

	// Sorted, so concurrent consumers write vm_state rows in the same order.
	keys := make([]string, 0, len(updates))
	for k := range updates {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]stateUpdate, len(keys))
	for i, k := range keys {
		out[i] = *updates[k]
	}
	return outcomes, transitions, out
}

// olderVersion reports whether resourceVersion a is older than b. Kubernetes
// calls resourceVersions opaque, but the API server takes them from etcd's
// revision counter, so in practice they are decimal integers that only grow.
// Anything else is never called older.
func olderVersion(a, b string) bool {
	x, errA := strconv.ParseUint(a, 10, 64)
	y, errB := strconv.ParseUint(b, 10, 64)
	return errA == nil && errB == nil && x < y
}

func orUnknown(s string) string {
	if s = strings.TrimSpace(s); s == "" {
		return "UNKNOWN"
	}
	return s
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

func loadConfig() (config, error) {
	cfg := config{
		ConsumerName: getenv("CONSUMER_NAME", "vm-event-consumer-1"),
		KafkaTopic:   getenv("KAFKA_TOPIC", "vm-events"),
		KafkaGroupID: getenv("KAFKA_GROUP_ID", "vm-event-consumer"),
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
	if cfg.MaxTransitionGap, err = durationEnv("MAX_TRANSITION_GAP", 30*time.Minute); err != nil {
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
	case cfg.MaxTransitionGap <= 0:
		return config{}, errors.New("MAX_TRANSITION_GAP must be positive")
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
	queries := []string{
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
			last_resource_version TEXT,
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
			resource_version TEXT,
			transition_at TIMESTAMPTZ NOT NULL,
			anomaly BOOLEAN NOT NULL DEFAULT false,
			reason TEXT
		)`,
		// Columns added with the Kafka-only consumer.
		`ALTER TABLE vm_state ADD COLUMN IF NOT EXISTS last_resource_version TEXT`,
		`ALTER TABLE vm_state_transitions ADD COLUMN IF NOT EXISTS resource_version TEXT`,
		`CREATE INDEX IF NOT EXISTS idx_vm_state_transitions_event_key ON vm_state_transitions(event_key, transition_at DESC)`,
		// payload is BYTEA because a rejected message may not be valid text or JSON.
		`CREATE TABLE IF NOT EXISTS vm_event_dead_letters (
			kafka_topic TEXT NOT NULL,
			kafka_partition INT NOT NULL,
			kafka_offset BIGINT NOT NULL,
			event_key TEXT,
			error TEXT NOT NULL,
			payload BYTEA NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
		)`,
	}

	for _, q := range queries {
		if _, err := pool.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

type pgStore struct {
	pool     *pgxpool.Pool
	consumer string        // CONSUMER_NAME, the dedupe scope in consumer_processed_events
	maxGap   time.Duration // MAX_TRANSITION_GAP
}

const insertProcessedSQL = `
INSERT INTO consumer_processed_events (consumer_name, event_fingerprint, event_id, event_key)
VALUES ($1, $2, $3, $4)
ON CONFLICT (consumer_name, event_fingerprint) DO NOTHING`

// Locks in key order, so concurrent consumers cannot deadlock on it.
const selectStatesSQL = `
SELECT event_key, COALESCE(last_status, ''), last_seen_at, COALESCE(last_resource_version, '')
FROM vm_state
WHERE event_key = ANY($1)
ORDER BY event_key
FOR UPDATE`

const insertTransitionSQL = `
INSERT INTO vm_state_transitions (
    consumer_name, event_key, from_status, to_status, event_type,
    event_id, resource_version, transition_at, anomaly, reason
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`

const upsertStateSQL = `
INSERT INTO vm_state (
    event_key, namespace, name, last_event_id, last_event_type, last_status,
    last_run_strategy, last_resource_version, last_seen_at, total_events
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (event_key) DO UPDATE SET
    last_event_id         = EXCLUDED.last_event_id,
    last_event_type       = EXCLUDED.last_event_type,
    last_status           = EXCLUDED.last_status,
    last_run_strategy     = EXCLUDED.last_run_strategy,
    last_resource_version = EXCLUDED.last_resource_version,
    last_seen_at          = EXCLUDED.last_seen_at,
    total_events          = vm_state.total_events + EXCLUDED.total_events,
    updated_at            = NOW()`

func (p pgStore) apply(ctx context.Context, rows []eventRow) ([]outcome, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	isNew, states, err := p.claim(ctx, tx, rows)
	if err != nil {
		return nil, err
	}
	outcomes, transitions, updates := plan(states, rows, isNew, p.maxGap)

	batch := &pgx.Batch{}
	for _, t := range transitions {
		batch.Queue(insertTransitionSQL,
			p.consumer, t.key, t.from, t.to, t.eventType,
			t.eventID, t.resourceVersion, t.at, t.anomaly, t.reason)
	}
	for _, u := range updates {
		batch.Queue(upsertStateSQL,
			u.key, u.namespace, u.name, u.eventID, u.eventType, u.status,
			u.runStrategy, u.resourceVersion, u.seenAt, u.events)
	}
	if batch.Len() > 0 {
		if err := tx.SendBatch(ctx, batch).Close(); err != nil {
			return nil, err
		}
	}
	return outcomes, tx.Commit(ctx)
}

// claim records rows as processed, reporting which ones are new, and reads
// the stored state of their VMs, locked until the transaction ends. It takes
// one round trip.
func (p pgStore) claim(ctx context.Context, tx pgx.Tx, rows []eventRow) ([]bool, map[string]vmState, error) {
	batch := &pgx.Batch{}
	var keys []string
	seen := make(map[string]bool)
	for _, row := range rows {
		batch.Queue(insertProcessedSQL, p.consumer, row.ev.EventFingerprint, row.msg.Offset, row.key)
		if !seen[row.key] {
			seen[row.key] = true
			keys = append(keys, row.key)
		}
	}
	batch.Queue(selectStatesSQL, keys)

	results := tx.SendBatch(ctx, batch)
	isNew := make([]bool, len(rows))
	for i := range rows {
		tag, err := results.Exec()
		if err != nil {
			_ = results.Close()
			return nil, nil, err
		}
		isNew[i] = tag.RowsAffected() == 1
	}

	stateRows, err := results.Query()
	if err != nil {
		_ = results.Close()
		return nil, nil, err
	}
	states := make(map[string]vmState, len(keys))
	for stateRows.Next() {
		var key string
		var s vmState
		if err := stateRows.Scan(&key, &s.status, &s.seenAt, &s.resourceVersion); err != nil {
			stateRows.Close()
			_ = results.Close()
			return nil, nil, err
		}
		states[key] = s
	}
	if err := stateRows.Err(); err != nil {
		_ = results.Close()
		return nil, nil, err
	}
	return isNew, states, results.Close()
}

func (p pgStore) deadLetter(ctx context.Context, msg kafka.Message, cause error) error {
	payload := msg.Value
	if payload == nil {
		payload = []byte{} // a Kafka tombstone; the column is NOT NULL
	}
	_, err := p.pool.Exec(ctx, `
INSERT INTO vm_event_dead_letters (kafka_topic, kafka_partition, kafka_offset, event_key, error, payload)
VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
`,
		msg.Topic,
		msg.Partition,
		msg.Offset,
		cleanText(string(msg.Key)),
		cleanText(cause.Error()),
		payload,
	)
	return err
}

// cleanText makes s fit a TEXT column, which rejects NUL bytes and invalid UTF-8.
func cleanText(s string) string {
	return strings.ToValidUTF8(strings.ReplaceAll(s, "\x00", ""), "�")
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
