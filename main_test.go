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

// fakeStore behaves like the PostgreSQL store: a batch is all or nothing and
// duplicate auditIDs are reported as not inserted.
type fakeStore struct {
	failures []error          // returned by the next insertEvents calls, in order
	down     error            // returned by every insertEvents call when set
	reject   map[string]error // fails any batch that contains this auditID
	stored   map[string]bool
	dead     []string
	attempts int
}

func newFakeStore() *fakeStore {
	return &fakeStore{reject: map[string]error{}, stored: map[string]bool{}}
}

func (s *fakeStore) insertEvents(_ context.Context, rows []eventRow) ([]bool, error) {
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
		if err := s.reject[row.ev.AuditID]; err != nil {
			return nil, err
		}
	}
	inserted := make([]bool, len(rows))
	for i, row := range rows {
		inserted[i] = !s.stored[row.ev.AuditID]
		s.stored[row.ev.AuditID] = true
	}
	return inserted, nil
}

func (s *fakeStore) deadLetter(_ context.Context, _ kafka.Message, auditID string, _ error) error {
	s.dead = append(s.dead, auditID)
	return nil
}

func newTestSink(r messageReader, st eventStore) *sink {
	return &sink{
		log:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		reader:       r,
		store:        st,
		batchMax:     100,
		retryInitial: time.Millisecond,
		retryMax:     5 * time.Millisecond,
	}
}

func vmEvent(id, group, subresource, verb string, code int32) auditEvent {
	return auditEvent{
		AuditID:        id,
		Stage:          "ResponseComplete",
		Verb:           verb,
		ObjectRef:      &objectRef{Resource: "virtualmachines", Namespace: "team-a", Name: "vm-1", APIGroup: group, Subresource: subresource},
		ResponseStatus: &responseStatus{Code: code},
		StageTimestamp: time.Now().Add(-50 * time.Millisecond),
		User:           userInfo{Username: "alice"},
	}
}

func message(t *testing.T, offset int64, ev auditEvent) kafka.Message {
	t.Helper()
	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Topic: "vm-audit-raw", Offset: offset, HighWaterMark: offset + 1, Value: b}
}

func createMsg(t *testing.T, offset int64, id string) kafka.Message {
	return message(t, offset, vmEvent(id, "kubevirt.io", "", "create", 201))
}

func TestWantEvent(t *testing.T) {
	withStage := vmEvent("x", "kubevirt.io", "", "create", 201)
	withStage.Stage = "RequestReceived"
	vmi := vmEvent("x", "kubevirt.io", "", "create", 201)
	vmi.ObjectRef.Resource = "virtualmachineinstances"

	tests := []struct {
		name string
		ev   auditEvent
		want bool
	}{
		{"create", vmEvent("x", "kubevirt.io", "", "create", 201), true},
		{"patch", vmEvent("x", "kubevirt.io", "", "patch", 200), true},
		{"delete collection", vmEvent("x", "kubevirt.io", "", "deletecollection", 200), true},
		{"status update", vmEvent("x", "kubevirt.io", "status", "update", 200), true},
		{"start via subresource API", vmEvent("x", "subresources.kubevirt.io", "start", "update", 202), true},
		{"get", vmEvent("x", "kubevirt.io", "", "get", 200), false},
		{"list", vmEvent("x", "kubevirt.io", "", "list", 200), false},
		{"watch", vmEvent("x", "kubevirt.io", "", "watch", 200), false},
		{"console", vmEvent("x", "subresources.kubevirt.io", "console", "get", 200), false},
		{"conflict", vmEvent("x", "kubevirt.io", "", "update", 409), false},
		{"other group", vmEvent("x", "example.com", "", "create", 201), false},
		{"request received stage", withStage, false},
		{"virtual machine instance", vmi, false},
		{"no objectRef", auditEvent{AuditID: "x", Stage: "ResponseComplete", Verb: "create"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := wantEvent(tt.ev); got != tt.want {
				t.Errorf("wantEvent() = %v, want %v", got, tt.want)
			}
		})
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

	ch <- kafka.Message{Offset: 3}
	close(ch)
	if batch, ok := nextBatch(ch, 2); !ok || len(batch) != 1 {
		t.Fatalf("after close: got %d messages, ok=%v; want the buffered one", len(batch), ok)
	}
	if _, ok := nextBatch(ch, 2); ok {
		t.Fatal("closed and empty channel: want ok=false")
	}
}

func TestHandleBatchStoresVMEventsThenCommits(t *testing.T) {
	st := newFakeStore()
	r := &fakeReader{}
	r.onCommit = func() {
		if len(st.stored) != 2 {
			t.Error("offsets committed before the VM events were stored")
		}
	}
	s := newTestSink(r, st)

	batch := []kafka.Message{
		createMsg(t, 0, "a"),
		{Offset: 1, Value: []byte(`{"auditID":"p","stage":"ResponseComplete","verb":"create","objectRef":{"resource":"pods"}}`)},
		message(t, 2, vmEvent("read", "kubevirt.io", "", "get", 200)),
		message(t, 3, vmEvent("b", "subresources.kubevirt.io", "start", "update", 202)),
	}
	if err := s.handleBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}

	if !st.stored["a"] || !st.stored["b"] || len(st.stored) != 2 {
		t.Errorf("stored = %v, want a and b", st.stored)
	}
	if st.attempts != 1 {
		t.Errorf("insert round trips = %d, want 1 for the whole batch", st.attempts)
	}
	if calls, maxOffset := r.committed(); calls != 1 || maxOffset != 3 {
		t.Errorf("commits = %d up to offset %d, want 1 up to offset 3", calls, maxOffset)
	}
}

func TestHandleBatchRetriesOutage(t *testing.T) {
	st := newFakeStore()
	st.failures = []error{
		errors.New("dial tcp 10.0.0.5:5432: connect: connection refused"),
		&pgconn.PgError{Code: "57P01", Message: "terminating connection due to administrator command"},
	}
	r := &fakeReader{}
	s := newTestSink(r, st)

	if err := s.handleBatch(context.Background(), []kafka.Message{createMsg(t, 7, "a")}); err != nil {
		t.Fatal(err)
	}
	if st.attempts != 3 || !st.stored["a"] {
		t.Errorf("attempts = %d, stored = %v; want 3 attempts and a stored", st.attempts, st.stored)
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
	s := newTestSink(r, st)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.handleBatch(ctx, []kafka.Message{createMsg(t, 0, "a")}); err == nil {
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
	st.reject["bad"] = &pgconn.PgError{Code: "22P05", Message: "unsupported Unicode escape sequence"}
	r := &fakeReader{}
	s := newTestSink(r, st)

	batch := []kafka.Message{createMsg(t, 0, "good-1"), createMsg(t, 1, "bad"), createMsg(t, 2, "good-2")}
	if err := s.handleBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}

	if !st.stored["good-1"] || !st.stored["good-2"] || st.stored["bad"] {
		t.Errorf("stored = %v, want good-1 and good-2 only", st.stored)
	}
	if len(st.dead) != 1 || st.dead[0] != "bad" {
		t.Errorf("dead-lettered = %v, want [bad]", st.dead)
	}
	if calls, maxOffset := r.committed(); calls != 1 || maxOffset != 2 {
		t.Errorf("commits = %d up to offset %d, want 1 up to offset 2", calls, maxOffset)
	}
}

func TestRunHandlesMessagesAndStops(t *testing.T) {
	st := newFakeStore()
	r := &fakeReader{queue: []kafka.Message{createMsg(t, 0, "a"), createMsg(t, 1, "b"), createMsg(t, 2, "c")}}
	s := newTestSink(r, st)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.run(ctx)
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
	if len(st.stored) != 3 {
		t.Errorf("stored = %v, want a, b and c", st.stored)
	}
}

func TestIsRowError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"NUL in JSONB", &pgconn.PgError{Code: "22P05"}, true},
		{"not null violation", &pgconn.PgError{Code: "23502"}, true},
		{"value too large", &pgconn.PgError{Code: "54000"}, true},
		{"wrapped data error", errors.Join(errors.New("batch"), &pgconn.PgError{Code: "22021"}), true},
		{"admin shutdown", &pgconn.PgError{Code: "57P01"}, false},
		{"disk full", &pgconn.PgError{Code: "53100"}, false},
		{"permission denied", &pgconn.PgError{Code: "42501"}, false},
		{"read-only transaction", &pgconn.PgError{Code: "25006"}, false},
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

func TestPartitionLag(t *testing.T) {
	tests := []struct {
		offset, highWaterMark, want int64
	}{
		{4, 10, 5},
		{9, 10, 0},
		{3, 0, 0},
	}
	for _, tt := range tests {
		got := partitionLag(kafka.Message{Offset: tt.offset, HighWaterMark: tt.highWaterMark})
		if got != tt.want {
			t.Errorf("partitionLag(offset %d, high water mark %d) = %d, want %d", tt.offset, tt.highWaterMark, got, tt.want)
		}
	}
}

func TestParseAuditEventEnvelope(t *testing.T) {
	inner := `{"auditID":"abc","stage":"ResponseComplete","verb":"create"}`
	wrapped, err := json.Marshal(map[string]string{"message": inner})
	if err != nil {
		t.Fatal(err)
	}
	ev, raw, ok := parseAuditEvent(wrapped)
	if !ok || ev.AuditID != "abc" || string(raw) != inner {
		t.Errorf("got %q, %q, %v; want the inner event", ev.AuditID, raw, ok)
	}
	if _, _, ok := parseAuditEvent([]byte("not json")); ok {
		t.Error("parsed a non-JSON message")
	}
}

func TestCleanText(t *testing.T) {
	if got := cleanText("a\x00b\xffc"); got != "ab�c" {
		t.Errorf("cleanText() = %q", got)
	}
}

func TestLoadConfig(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://localhost/test")
	t.Setenv("KAFKA_BROKERS", "broker-0:9092, broker-1:9092")

	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.KafkaCommitInterval != time.Second || cfg.BatchMaxSize != 500 || len(cfg.KafkaBrokers) != 2 {
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
