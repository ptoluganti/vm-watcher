package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	kafka "github.com/segmentio/kafka-go"
)

// fakeReader serves queue from FetchMessage, then blocks until ctx is done.
type fakeReader struct {
	mu       sync.Mutex
	queue    []kafka.Message
	commits  [][]kafka.Message
	onCommit func()
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.mu.Lock()
	if len(r.queue) > 0 {
		msg := r.queue[0]
		r.queue = r.queue[1:]
		r.mu.Unlock()
		return msg, nil
	}
	r.mu.Unlock()
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	if r.onCommit != nil {
		r.onCommit()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commits = append(r.commits, append([]kafka.Message(nil), msgs...))
	return nil
}

func (r *fakeReader) committed() (calls int, maxOffset int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	maxOffset = -1
	for _, c := range r.commits {
		for _, msg := range c {
			maxOffset = max(maxOffset, msg.Offset)
		}
	}
	return len(r.commits), maxOffset
}

// fakeStore behaves like the PostgreSQL store: a batch is all or nothing,
// fingerprints already processed are duplicates, and states change through
// plan.
type fakeStore struct {
	mu          sync.Mutex
	failures    []error          // returned by the next apply calls, in order
	down        error            // returned by every apply call when set
	reject      map[string]error // fails any batch that contains this fingerprint
	processed   map[string]bool
	states      map[string]vmState
	transitions []transition
	dead        []int64 // offsets
	attempts    int
}

func newFakeStore() *fakeStore {
	return &fakeStore{reject: map[string]error{}, processed: map[string]bool{}, states: map[string]vmState{}}
}

func (s *fakeStore) apply(_ context.Context, rows []eventRow) ([]outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if s.down != nil {
		return nil, s.down
	}
	if len(s.failures) > 0 {
		err := s.failures[0]
		s.failures = s.failures[1:]
		return nil, err
	}
	for _, row := range rows {
		if err := s.reject[row.ev.EventFingerprint]; err != nil {
			return nil, err
		}
	}
	isNew := make([]bool, len(rows))
	for i, row := range rows {
		isNew[i] = !s.processed[row.ev.EventFingerprint]
		s.processed[row.ev.EventFingerprint] = true
	}
	outcomes, transitions, _ := plan(s.states, rows, isNew, time.Hour)
	s.transitions = append(s.transitions, transitions...)
	return outcomes, nil
}

func (s *fakeStore) deadLetter(_ context.Context, msg kafka.Message, _ error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dead = append(s.dead, msg.Offset)
	return nil
}

func (s *fakeStore) state(key string) (vmState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.states[key]
	return st, ok
}

func newTestConsumer(r messageReader, st stateStore) *consumer {
	return &consumer{
		log:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		reader:       r,
		store:        st,
		batchMax:     100,
		retryInitial: time.Millisecond,
		retryMax:     5 * time.Millisecond,
	}
}

var t0 = time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)

// testEvent is a change to team-a/vm-1; the fingerprint is unique per
// type and resourceVersion, as the watcher's is.
func testEvent(eventType, status, rv string) vmEvent {
	return vmEvent{
		Type:             eventType,
		Namespace:        "team-a",
		Name:             "vm-1",
		UID:              "uid-1",
		ResourceVersion:  rv,
		PrintableStatus:  status,
		EventFingerprint: eventType + "|" + rv,
		Timestamp:        time.Now().Add(-20 * time.Millisecond),
	}
}

func message(t *testing.T, offset int64, ev vmEvent) kafka.Message {
	t.Helper()
	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{
		Topic:         "vm-events",
		Offset:        offset,
		HighWaterMark: offset + 1,
		Key:           []byte(ev.Namespace + "/" + ev.Name),
		Value:         b,
		Time:          ev.Timestamp,
	}
}

func testRow(eventType, status, rv string, offset int64, at time.Time) eventRow {
	ev := testEvent(eventType, status, rv)
	ev.Timestamp = at
	return eventRow{msg: kafka.Message{Offset: offset}, key: "team-a/vm-1", ev: ev}
}

func TestPlanAppliesInOrderAndSkipsOlderVersions(t *testing.T) {
	states := map[string]vmState{"team-a/vm-1": {status: "Stopped", seenAt: t0, resourceVersion: "100"}}
	other := testRow("ADDED", "Running", "50", 5, t0.Add(5*time.Second))
	other.key = "team-b/vm-2"
	other.ev.Namespace, other.ev.Name = "team-b", "vm-2"
	rows := []eventRow{
		testRow("MODIFIED", "Starting", "101", 1, t0.Add(time.Second)),
		testRow("MODIFIED", "Running", "102", 2, t0.Add(2*time.Second)),
		testRow("MODIFIED", "Starting", "101", 3, t0.Add(time.Second)), // redelivered
		testRow("MODIFIED", "Stopped", "99", 4, t0.Add(4*time.Second)), // delivered late
		other,
	}
	isNew := []bool{true, true, false, true, true}

	outcomes, transitions, updates := plan(states, rows, isNew, time.Hour)

	wantOutcomes := []outcome{applied, applied, duplicate, stale, applied}
	for i, want := range wantOutcomes {
		if outcomes[i] != want {
			t.Errorf("row %d outcome = %v, want %v", i, outcomes[i], want)
		}
	}
	if len(transitions) != 2 ||
		transitions[0].from != "Stopped" || transitions[0].to != "Starting" || transitions[0].resourceVersion != "101" ||
		transitions[1].from != "Starting" || transitions[1].to != "Running" || transitions[1].eventID != 2 {
		t.Errorf("transitions = %+v, want Stopped->Starting (101), Starting->Running (offset 2)", transitions)
	}
	if len(updates) != 2 || updates[0].key != "team-a/vm-1" || updates[1].key != "team-b/vm-2" {
		t.Fatalf("updates = %+v, want one per VM, sorted by key", updates)
	}
	if u := updates[0]; u.status != "Running" || u.resourceVersion != "102" || u.events != 2 || u.eventID != 2 ||
		u.namespace != "team-a" || u.name != "vm-1" {
		t.Errorf("team-a/vm-1 update = %+v, want Running at 102 after 2 events", u)
	}
	if st := states["team-a/vm-1"]; st.status != "Running" || st.resourceVersion != "102" {
		t.Errorf("state = %+v, want Running at 102", st)
	}
}

func TestPlanTieOrUnknownVersionApplies(t *testing.T) {
	states := map[string]vmState{"team-a/vm-1": {status: "Running", seenAt: t0, resourceVersion: "100"}}
	rows := []eventRow{
		// A tombstone for a delete missed during a re-list carries the last
		// state the watcher saw, so its resourceVersion equals the stored one.
		testRow("DELETED", "Running", "100", 1, t0.Add(time.Second)),
		testRow("ADDED", "Stopped", "not-a-number", 2, t0.Add(2*time.Second)),
	}
	outcomes, _, updates := plan(states, rows, []bool{true, true}, time.Hour)
	if outcomes[0] != applied || outcomes[1] != applied {
		t.Errorf("outcomes = %v, want both applied", outcomes)
	}
	if len(updates) != 1 || updates[0].events != 2 || updates[0].eventType != "ADDED" {
		t.Errorf("updates = %+v", updates)
	}
}

func TestPlanFlagsAnomalies(t *testing.T) {
	states := map[string]vmState{"team-a/vm-1": {status: "Stopped", seenAt: t0, resourceVersion: "100"}}
	rows := []eventRow{
		testRow("MODIFIED", "Running", "101", 1, t0.Add(time.Second)),  // skipped Starting
		testRow("MODIFIED", "Stopping", "102", 2, t0.Add(2*time.Hour)), // long gap
		testRow("MODIFIED", "", "103", 3, t0.Add(2*time.Hour)),         // unknown status
	}
	_, transitions, _ := plan(states, rows, []bool{true, true, true}, time.Hour)
	if len(transitions) != 3 {
		t.Fatalf("transitions = %+v, want 3", transitions)
	}
	if !transitions[0].anomaly || transitions[0].reason != "unexpected_status_transition" {
		t.Errorf("Stopped->Running = %+v, want unexpected_status_transition", transitions[0])
	}
	if !transitions[1].anomaly || transitions[1].reason != "large_transition_gap" {
		t.Errorf("Running->Stopping after 2h = %+v, want large_transition_gap", transitions[1])
	}
	if transitions[2].to != "UNKNOWN" || transitions[2].anomaly {
		t.Errorf("transition to empty status = %+v, want UNKNOWN and no anomaly", transitions[2])
	}
}

func TestOlderVersion(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{"99", "100", true},
		{"100", "100", false},
		{"101", "100", false},
		{"", "100", false},
		{"100", "", false},
		{"abc", "100", false},
	}
	for _, tt := range tests {
		if got := olderVersion(tt.a, tt.b); got != tt.want {
			t.Errorf("olderVersion(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestHandleBatchAppliesThenCommits(t *testing.T) {
	st := newFakeStore()
	r := &fakeReader{}
	r.onCommit = func() {
		if _, ok := st.state("team-a/vm-1"); !ok {
			t.Error("offsets committed before the VM state was stored")
		}
	}
	c := newTestConsumer(r, st)

	batch := []kafka.Message{
		message(t, 0, testEvent("ADDED", "Stopped", "100")),
		message(t, 1, testEvent("MODIFIED", "Starting", "101")),
		message(t, 2, testEvent("MODIFIED", "Running", "102")),
	}
	if err := c.handleBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}

	if s, _ := st.state("team-a/vm-1"); s.status != "Running" || s.resourceVersion != "102" {
		t.Errorf("state = %+v, want Running at 102", s)
	}
	if len(st.transitions) != 2 {
		t.Errorf("transitions = %+v, want Stopped->Starting->Running", st.transitions)
	}
	if st.attempts != 1 {
		t.Errorf("apply round trips = %d, want 1 for the whole batch", st.attempts)
	}
	if calls, maxOffset := r.committed(); calls != 1 || maxOffset != 2 {
		t.Errorf("commits = %d up to offset %d, want 1 up to offset 2", calls, maxOffset)
	}
}

func TestHandleBatchRetriesOutage(t *testing.T) {
	st := newFakeStore()
	st.failures = []error{
		errors.New("dial tcp 10.0.0.5:5432: connect: connection refused"),
		&pgconn.PgError{Code: "40P01", Message: "deadlock detected"},
	}
	r := &fakeReader{}
	c := newTestConsumer(r, st)

	if err := c.handleBatch(context.Background(), []kafka.Message{message(t, 7, testEvent("ADDED", "Stopped", "100"))}); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.state("team-a/vm-1"); st.attempts != 3 || !ok {
		t.Errorf("attempts = %d, stored = %v; want 3 attempts and the state stored", st.attempts, ok)
	}
	if len(st.dead) != 0 {
		t.Errorf("dead-lettered %v during an outage, want none", st.dead)
	}
	if calls, maxOffset := r.committed(); calls != 1 || maxOffset != 7 {
		t.Errorf("commits = %d up to offset %d, want 1 up to offset 7", calls, maxOffset)
	}
}

func TestHandleBatchDoesNotCommitDuringOutage(t *testing.T) {
	st := newFakeStore()
	st.down = errors.New("connection refused")
	r := &fakeReader{}
	c := newTestConsumer(r, st)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := c.handleBatch(ctx, []kafka.Message{message(t, 0, testEvent("ADDED", "Stopped", "100"))}); err == nil {
		t.Fatal("handleBatch returned nil while PostgreSQL was down, want the shutdown error")
	}
	if st.attempts < 2 {
		t.Errorf("attempts = %d, want the write retried", st.attempts)
	}
	if calls, _ := r.committed(); calls != 0 {
		t.Errorf("committed %d times without storing the event, want 0", calls)
	}
}

func TestHandleBatchDeadLettersRejectedRow(t *testing.T) {
	st := newFakeStore()
	bad := testEvent("MODIFIED", "Starting", "101")
	st.reject[bad.EventFingerprint] = &pgconn.PgError{Code: "22021", Message: "invalid byte sequence for encoding \"UTF8\""}
	r := &fakeReader{}
	c := newTestConsumer(r, st)

	batch := []kafka.Message{
		message(t, 0, testEvent("ADDED", "Stopped", "100")),
		message(t, 1, bad),
		message(t, 2, testEvent("MODIFIED", "Running", "102")),
	}
	if err := c.handleBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}

	if s, _ := st.state("team-a/vm-1"); s.status != "Running" {
		t.Errorf("state = %+v, want Running from the event after the rejected one", s)
	}
	if len(st.dead) != 1 || st.dead[0] != 1 {
		t.Errorf("dead-lettered offsets = %v, want [1]", st.dead)
	}
	if calls, maxOffset := r.committed(); calls != 1 || maxOffset != 2 {
		t.Errorf("commits = %d up to offset %d, want 1 up to offset 2", calls, maxOffset)
	}
}

func TestHandleBatchDeadLettersUnusableMessages(t *testing.T) {
	st := newFakeStore()
	r := &fakeReader{}
	c := newTestConsumer(r, st)

	noFingerprint := testEvent("ADDED", "Stopped", "100")
	noFingerprint.EventFingerprint = ""
	noName := testEvent("ADDED", "Stopped", "100")
	noName.Namespace, noName.Name = "", ""
	noNameMsg := message(t, 3, noName)
	noNameMsg.Key = nil

	batch := []kafka.Message{
		{Topic: "vm-events", Offset: 0, Key: []byte("team-a/vm-1"), Value: []byte("not json")},
		{Topic: "vm-events", Offset: 1, Key: []byte("team-a/vm-1")}, // tombstone: no value
		message(t, 2, noFingerprint),
		noNameMsg,
		message(t, 4, testEvent("ADDED", "Stopped", "100")),
	}
	if err := c.handleBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}

	if len(st.dead) != 4 {
		t.Errorf("dead-lettered offsets = %v, want 0-3", st.dead)
	}
	if _, ok := st.state("team-a/vm-1"); !ok {
		t.Error("the valid event was not applied")
	}
	if calls, maxOffset := r.committed(); calls != 1 || maxOffset != 4 {
		t.Errorf("commits = %d up to offset %d, want 1 up to offset 4", calls, maxOffset)
	}
}

func TestHandleBatchCountsRedeliveryAsDuplicate(t *testing.T) {
	st := newFakeStore()
	r := &fakeReader{}
	c := newTestConsumer(r, st)

	msg := message(t, 0, testEvent("ADDED", "Stopped", "100"))
	for i := 0; i < 2; i++ {
		if err := c.handleBatch(context.Background(), []kafka.Message{msg}); err != nil {
			t.Fatal(err)
		}
	}
	if len(st.processed) != 1 || len(st.transitions) != 0 {
		t.Errorf("processed = %v, transitions = %v; want one event and no transition", st.processed, st.transitions)
	}
}

func TestRunHandlesMessagesAndStops(t *testing.T) {
	st := newFakeStore()
	r := &fakeReader{queue: []kafka.Message{
		message(t, 0, testEvent("ADDED", "Stopped", "100")),
		message(t, 1, testEvent("MODIFIED", "Starting", "101")),
		message(t, 2, testEvent("MODIFIED", "Running", "102")),
	}}
	c := newTestConsumer(r, st)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.run(ctx)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, maxOffset := r.committed(); maxOffset == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("messages were not committed")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("run did not return after shutdown")
	}
	if s, _ := st.state("team-a/vm-1"); s.status != "Running" {
		t.Errorf("state = %+v, want Running", s)
	}
}

func TestNextBatch(t *testing.T) {
	ch := make(chan kafka.Message, 10)
	for i := 0; i < 3; i++ {
		ch <- kafka.Message{Offset: int64(i)}
	}

	if batch, ok := nextBatch(ch, 2); !ok || len(batch) != 2 {
		t.Fatalf("first batch: got %d messages, ok=%v; want 2, true", len(batch), ok)
	}
	// Only one message is waiting: the batch is returned without waiting for more.
	if batch, ok := nextBatch(ch, 2); !ok || len(batch) != 1 || batch[0].Offset != 2 {
		t.Fatalf("second batch: got %v, ok=%v; want offset 2 only", batch, ok)
	}
	close(ch)
	if _, ok := nextBatch(ch, 2); ok {
		t.Fatal("closed and empty channel: want ok=false")
	}
}

func TestIsRowError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"invalid UTF-8", &pgconn.PgError{Code: "22021"}, true},
		{"not null violation", &pgconn.PgError{Code: "23502"}, true},
		{"deadlock", &pgconn.PgError{Code: "40P01"}, false},
		{"admin shutdown", &pgconn.PgError{Code: "57P01"}, false},
		{"network error", errors.New("connection refused"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRowError(tt.err); got != tt.want {
				t.Errorf("isRowError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCleanText(t *testing.T) {
	if got := cleanText("a\x00b\xffc"); got != "ab�c" {
		t.Errorf("cleanText() = %q", got)
	}
}

func TestLoadConfig(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://localhost/test")
	t.Setenv("KAFKA_BROKERS", "")
	if _, err := loadConfig(); err == nil {
		t.Error("KAFKA_BROKERS unset accepted: the consumer only reads Kafka")
	}

	t.Setenv("KAFKA_BROKERS", "broker-0:9092, broker-1:9092")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.KafkaTopic != "vm-events" || cfg.KafkaGroupID != "vm-event-consumer" ||
		cfg.KafkaCommitInterval != time.Second || cfg.BatchMaxSize != 500 || len(cfg.KafkaBrokers) != 2 {
		t.Errorf("defaults = %+v", cfg)
	}

	t.Setenv("KAFKA_COMMIT_INTERVAL", "0")
	if cfg, err := loadConfig(); err != nil || cfg.KafkaCommitInterval != 0 {
		t.Errorf("KAFKA_COMMIT_INTERVAL=0: got %v, %v; want synchronous commits allowed", cfg.KafkaCommitInterval, err)
	}

	t.Setenv("BATCH_MAX_SIZE", "0")
	if _, err := loadConfig(); err == nil {
		t.Error("BATCH_MAX_SIZE=0 accepted")
	}
}
