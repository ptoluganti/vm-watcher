package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func testVM(rv string, generation int64, status string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "kubevirt.io/v1",
		"kind":       "VirtualMachine",
		"metadata": map[string]interface{}{
			"namespace":       "team-a",
			"name":            "vm-1",
			"uid":             "uid-1",
			"resourceVersion": rv,
			"generation":      generation,
		},
		"spec": map[string]interface{}{
			"runStrategy": "Always",
			"template": map[string]interface{}{"spec": map[string]interface{}{
				"volumes": []interface{}{map[string]interface{}{"name": "rootdisk"}},
			}},
		},
		"status": map[string]interface{}{"printableStatus": status},
	}}
}

// withRV returns a copy of u at a new resourceVersion, changed by edit.
func withRV(u *unstructured.Unstructured, rv string, edit func(*unstructured.Unstructured)) *unstructured.Unstructured {
	c := u.DeepCopy()
	c.SetResourceVersion(rv)
	edit(c)
	return c
}

func newTestController(events chan queuedEvent, done chan struct{}) *controller {
	return &controller{events: events, done: done, log: discard, now: time.Now}
}

func drain(events chan queuedEvent) []queuedEvent {
	var out []queuedEvent
	for {
		select {
		case ev := <-events:
			out = append(out, ev)
		default:
			return out
		}
	}
}

func TestHandlersPublishChangesInOrder(t *testing.T) {
	events := make(chan queuedEvent, 10)
	h := newTestController(events, make(chan struct{})).handlers()

	stopped := testVM("100", 1, "Stopped")
	running := withRV(stopped, "101", func(u *unstructured.Unstructured) {
		_ = unstructured.SetNestedField(u.Object, "Running", "status", "printableStatus")
	})
	bookkeeping := withRV(running, "102", func(u *unstructured.Unstructured) {
		_ = unstructured.SetNestedField(u.Object, int64(1), "status", "observedGeneration")
	})
	labelled := withRV(bookkeeping, "103", func(u *unstructured.Unstructured) {
		u.SetLabels(map[string]string{"env": "demo"})
	})

	h.OnAdd(stopped, false)
	h.OnUpdate(stopped, running)
	h.OnUpdate(running, running.DeepCopy()) // resync: same resourceVersion
	h.OnUpdate(running, bookkeeping)        // status bookkeeping only
	h.OnUpdate(bookkeeping, labelled)
	h.OnDelete(cache.DeletedFinalStateUnknown{Key: "team-a/vm-1", Obj: labelled})

	got := drain(events)
	want := []struct {
		eventType EventType
		rv        string
	}{
		{EventAdded, "100"},
		{EventUpdated, "101"},
		{EventUpdated, "103"},
		{EventDeleted, "103"},
	}
	if len(got) != len(want) {
		t.Fatalf("published %d events, want %d", len(got), len(want))
	}
	for i, w := range want {
		var ev VMEvent
		if err := json.Unmarshal(got[i].msg.Value, &ev); err != nil {
			t.Fatal(err)
		}
		if ev.Type != w.eventType || ev.ResourceVersion != w.rv || got[i].eventType != w.eventType {
			t.Errorf("event %d = %s at resourceVersion %s, want %s at %s", i, ev.Type, ev.ResourceVersion, w.eventType, w.rv)
		}
		if string(got[i].msg.Key) != "team-a/vm-1" {
			t.Errorf("event %d key = %q, want team-a/vm-1", i, got[i].msg.Key)
		}
		if ev.EventFingerprint == "" || ev.Timestamp.IsZero() || !got[i].msg.Time.Equal(ev.Timestamp) {
			t.Errorf("event %d: fingerprint %q, timestamp %v, message time %v", i, ev.EventFingerprint, ev.Timestamp, got[i].msg.Time)
		}
	}
}

func TestNewVMEventPayload(t *testing.T) {
	u := testVM("100", 3, "Running")
	u.SetLabels(map[string]string{"app": "web"})
	ev := newVMEvent(EventAdded, u, time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC))

	if ev.Namespace != "team-a" || ev.Name != "vm-1" || ev.UID != "uid-1" || ev.Generation != 3 {
		t.Errorf("identity = %s/%s uid %s generation %d", ev.Namespace, ev.Name, ev.UID, ev.Generation)
	}
	if ev.RunStrategy != "Always" || ev.PrintableStatus != "Running" || ev.Labels["app"] != "web" {
		t.Errorf("state = runStrategy %q status %q labels %v", ev.RunStrategy, ev.PrintableStatus, ev.Labels)
	}
	if !strings.Contains(string(ev.Disks), "rootdisk") || len(ev.Spec) == 0 || len(ev.StatusObject) == 0 {
		t.Errorf("disks %s, spec %d bytes, status %d bytes", ev.Disks, len(ev.Spec), len(ev.StatusObject))
	}

	noVolumes := testVM("100", 3, "Running")
	unstructured.RemoveNestedField(noVolumes.Object, "spec", "template")
	if ev := newVMEvent(EventAdded, noVolumes, time.Now()); ev.Disks != nil {
		t.Errorf("disks = %s for a VM without volumes, want none", ev.Disks)
	}
}

func TestEventFingerprintIgnoresObservationTime(t *testing.T) {
	u := testVM("100", 1, "Running")
	a := newVMEvent(EventUpdated, u, time.Now())
	b := newVMEvent(EventUpdated, u, time.Now().Add(time.Minute))
	if a.EventFingerprint != b.EventFingerprint {
		t.Error("the same change observed twice got different fingerprints")
	}
	if c := newVMEvent(EventAdded, u, time.Now()); c.EventFingerprint == a.EventFingerprint {
		t.Error("ADDED and MODIFIED at the same resourceVersion share a fingerprint")
	}
}

func TestSignificantState(t *testing.T) {
	base := testVM("100", 1, "Running")
	tests := []struct {
		name string
		edit func(*unstructured.Unstructured)
		want bool // significant
	}{
		{"spec change", func(u *unstructured.Unstructured) { u.SetGeneration(2) }, true},
		{"label", func(u *unstructured.Unstructured) { u.SetLabels(map[string]string{"a": "b"}) }, true},
		{"annotation", func(u *unstructured.Unstructured) { u.SetAnnotations(map[string]string{"a": "b"}) }, true},
		{"deletion", func(u *unstructured.Unstructured) { now := metav1.Now(); u.SetDeletionTimestamp(&now) }, true},
		{"printable status", func(u *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(u.Object, "Stopping", "status", "printableStatus")
		}, true},
		{"ready condition", func(u *unstructured.Unstructured) {
			_ = unstructured.SetNestedSlice(u.Object, []interface{}{
				map[string]interface{}{"type": "Ready", "status": "True"},
			}, "status", "conditions")
		}, true},
		{"observedGeneration", func(u *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(u.Object, int64(1), "status", "observedGeneration")
		}, false},
		{"other condition", func(u *unstructured.Unstructured) {
			_ = unstructured.SetNestedSlice(u.Object, []interface{}{
				map[string]interface{}{"type": "AgentConnected", "status": "True"},
			}, "status", "conditions")
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changed := withRV(base, "101", tt.edit)
			if got := significantState(base) != significantState(changed); got != tt.want {
				t.Errorf("significant = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEnqueueDropsEventTooLargeForKafka(t *testing.T) {
	events := make(chan queuedEvent, 1)
	c := newTestController(events, make(chan struct{}))
	u := testVM("100", 1, "Running")
	u.SetAnnotations(map[string]string{"huge": strings.Repeat("x", maxEventBytes)})

	c.enqueue(EventAdded, u)
	if len(events) != 0 {
		t.Error("queued an event Kafka would reject")
	}
}

func TestEnqueueStopsWaitingOnShutdown(t *testing.T) {
	done := make(chan struct{})
	c := newTestController(make(chan queuedEvent), done) // nobody reads: always full
	close(done)

	returned := make(chan struct{})
	go func() {
		c.enqueue(EventAdded, testVM("100", 1, "Running"))
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("enqueue kept waiting on a full queue after shutdown")
	}
}

// fakeWriter returns errs from successive WriteMessages calls (nil after the
// last one) and records every call.
type fakeWriter struct {
	mu     sync.Mutex
	errs   []error
	always error
	calls  [][]kafka.Message
}

func (w *fakeWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls = append(w.calls, append([]kafka.Message(nil), msgs...))
	if w.always != nil {
		return w.always
	}
	if len(w.errs) > 0 {
		err := w.errs[0]
		w.errs = w.errs[1:]
		return err
	}
	return nil
}

func (w *fakeWriter) keys() [][]string {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out [][]string
	for _, call := range w.calls {
		var keys []string
		for _, msg := range call {
			keys = append(keys, string(msg.Key))
		}
		out = append(out, keys)
	}
	return out
}

func newTestPublisher(w messageWriter, batchMax int) *publisher {
	return &publisher{
		log:          discard,
		writer:       w,
		batchMax:     batchMax,
		retryInitial: time.Millisecond,
		retryMax:     5 * time.Millisecond,
	}
}

func queued(keys ...string) []queuedEvent {
	out := make([]queuedEvent, len(keys))
	for i, k := range keys {
		out[i] = queuedEvent{msg: kafka.Message{Key: []byte(k), Value: []byte("{}"), Time: time.Now()}, eventType: EventUpdated, namespace: "team-a"}
	}
	return out
}

func TestPublishRetriesUntilWritten(t *testing.T) {
	w := &fakeWriter{errs: []error{errors.New("dial tcp: connection refused"), kafka.LeaderNotAvailable}}
	p := newTestPublisher(w, 10)

	if err := p.publish(context.Background(), queued("a", "b")); err != nil {
		t.Fatal(err)
	}
	if got := w.keys(); len(got) != 3 || strings.Join(got[2], ",") != "a,b" {
		t.Errorf("writes = %v, want the whole batch written 3 times", got)
	}
}

func TestPublishRetriesOnlyUnwrittenMessages(t *testing.T) {
	w := &fakeWriter{errs: []error{kafka.WriteErrors{nil, kafka.NotLeaderForPartition, nil}}}
	p := newTestPublisher(w, 10)

	if err := p.publish(context.Background(), queued("a", "b", "c")); err != nil {
		t.Fatal(err)
	}
	if got := w.keys(); len(got) != 2 || strings.Join(got[1], ",") != "b" {
		t.Errorf("writes = %v, want only b retried", got)
	}
}

func TestPublishStopsWhenContextDone(t *testing.T) {
	w := &fakeWriter{always: errors.New("connection refused")}
	p := newTestPublisher(w, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := p.publish(ctx, queued("a")); err == nil {
		t.Fatal("publish returned nil while Kafka was down, want the context error")
	}
	if got := w.keys(); len(got) < 2 {
		t.Errorf("writes = %d, want the write retried", len(got))
	}
}

func TestRunWritesInOrderThenStops(t *testing.T) {
	w := &fakeWriter{}
	p := newTestPublisher(w, 2)
	events := make(chan queuedEvent, 5)
	for _, ev := range queued("a", "b", "c", "d", "e") {
		events <- ev
	}
	close(events)

	done := make(chan struct{})
	go func() {
		p.run(context.Background(), events)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("run did not return after the queue was closed and drained")
	}

	var order []string
	for _, call := range w.keys() {
		if len(call) > 2 {
			t.Errorf("batch %v is larger than BATCH_MAX_SIZE 2", call)
		}
		order = append(order, call...)
	}
	if strings.Join(order, ",") != "a,b,c,d,e" {
		t.Errorf("written = %v, want a,b,c,d,e in order", order)
	}
}

func TestNextBatch(t *testing.T) {
	ch := make(chan queuedEvent, 10)
	for _, ev := range queued("a", "b", "c") {
		ch <- ev
	}

	if batch, ok := nextBatch(ch, 2); !ok || len(batch) != 2 {
		t.Fatalf("first batch: got %d events, ok=%v; want 2, true", len(batch), ok)
	}
	// Only one event is waiting: the batch is returned without waiting for more.
	if batch, ok := nextBatch(ch, 2); !ok || len(batch) != 1 || string(batch[0].msg.Key) != "c" {
		t.Fatalf("second batch: got %d events, ok=%v; want c only", len(batch), ok)
	}
	close(ch)
	if _, ok := nextBatch(ch, 2); ok {
		t.Fatal("closed and empty channel: want ok=false")
	}
}

// The writer settings that keep a lone VM change at about 10 ms to Kafka.
func TestNewWriterLatencySettings(t *testing.T) {
	w := newWriter(config{KafkaBrokers: []string{"broker-0:9092"}, KafkaTopic: "vm-events", BatchMaxSize: 100}, discard)

	if _, ok := w.Balancer.(*kafka.Hash); !ok {
		t.Errorf("balancer = %T, want *kafka.Hash so each VM keeps one partition", w.Balancer)
	}
	if w.BatchTimeout != 5*time.Millisecond || w.BatchSize != 100 || w.RequiredAcks != kafka.RequireAll {
		t.Errorf("batch timeout %v, size %d, acks %v; want 5ms, 100, RequireAll", w.BatchTimeout, w.BatchSize, w.RequiredAcks)
	}
	tr, ok := w.Transport.(*kafka.Transport)
	if !ok || tr.IdleTimeout != kafkaIdleTimeout {
		t.Fatalf("transport = %#v, want a *kafka.Transport keeping idle connections %v", w.Transport, kafkaIdleTimeout)
	}
	if kafkaIdleTimeout < time.Minute || kafkaIdleTimeout >= 10*time.Minute {
		t.Errorf("kafkaIdleTimeout = %v, want minutes but under the brokers' 10 min connections.max.idle.ms", kafkaIdleTimeout)
	}
}

func TestParseWatchNamespaces(t *testing.T) {
	tests := []struct {
		in       string
		watchAll bool
		want     string
	}{
		{"", true, ""},
		{"*", true, ""},
		{"ALL", true, ""},
		{"team-a, team-b,team-a", false, "team-a,team-b"},
		{" , ", true, ""},
	}
	for _, tt := range tests {
		watchAll, namespaces := parseWatchNamespaces(tt.in)
		if watchAll != tt.watchAll || strings.Join(namespaces, ",") != tt.want {
			t.Errorf("parseWatchNamespaces(%q) = %v, %v; want %v, %q", tt.in, watchAll, namespaces, tt.watchAll, tt.want)
		}
	}
}

func TestLoadConfig(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")
	if _, err := loadConfig(); err == nil {
		t.Error("KAFKA_BROKERS unset accepted")
	}

	t.Setenv("KAFKA_BROKERS", "broker-0:9092, broker-1:9092")
	t.Setenv("WATCH_NAMESPACES", "team-a,team-b")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.KafkaTopic != "vm-events" || cfg.BatchMaxSize != 100 || cfg.ResyncPeriod != 0 ||
		len(cfg.KafkaBrokers) != 2 || cfg.WatchAll || len(cfg.Namespaces) != 2 {
		t.Errorf("config = %+v", cfg)
	}

	t.Setenv("BATCH_MAX_SIZE", "0")
	if _, err := loadConfig(); err == nil {
		t.Error("BATCH_MAX_SIZE=0 accepted")
	}
}
