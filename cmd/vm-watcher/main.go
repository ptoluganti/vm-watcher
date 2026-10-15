// vm-watcher publishes KubeVirt VirtualMachine changes to Kafka as they
// happen. It is the live VM state lane; main.go is the audit lane.
//
// Watch, not audit log:
// A watch delivers a change tens of milliseconds after the API server commits
// it, without the audit lane's log file and collector hops. It does not say
// who made the change: join to the audit lane on namespace/name and
// resourceVersion for that (README.md, "Joining the two lanes").
//
// one ordered stream per VM:
// Messages are keyed by namespace/name, so the Hash balancer sends each VM's
// changes to one partition. A single publisher writes them in the order the
// informer delivered them, and retries a failed write before writing anything
// newer.
//
// micro-batches:
// Each Kafka write is whatever is already queued (up to BATCH_MAX_SIZE).
// BatchTimeout is 5ms instead of kafka-go's 1s, and RequiredAcks waits for
// every in-sync replica, so an acknowledged change survives a broker failure.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	kafka "github.com/segmentio/kafka-go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

// Swap to VirtualMachineInstance if you want runtime (pod-level) lifecycle:
// Resource: "virtualmachineinstances"
var vmGVR = schema.GroupVersionResource{
	Group:    "kubevirt.io",
	Version:  "v1",
	Resource: "virtualmachines",
}

type EventType string

const (
	EventAdded   EventType = "ADDED"
	EventUpdated EventType = "MODIFIED"
	EventDeleted EventType = "DELETED"
)

const (
	// maxEventBytes keeps a message under Kafka's default 1 MiB limits
	// (broker message.max.bytes, kafka-go BatchBytes). A larger one could
	// never be written, and retrying it would hold back every later event.
	maxEventBytes = 1_000_000

	// drainTimeout is how long queued events are still written after
	// shutdown starts. It fits in the pod's 30s termination grace period.
	drainTimeout = 10 * time.Second

	// kafkaIdleTimeout is how long an unused broker connection stays open
	// (kafka-go's default is 30s). It is just under the brokers' own
	// connections.max.idle.ms of 10 min, so the watcher closes it first.
	kafkaIdleTimeout = 9 * time.Minute
)

var (
	vmEventsObservedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vm_events_observed_total",
		Help: "VM watch events queued for Kafka.",
	}, []string{"type", "namespace"})

	vmEventsPublishedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vm_events_published_total",
		Help: "VM events acknowledged by Kafka.",
	}, []string{"type", "namespace"})

	vmEventsPublishFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vm_events_publish_failures_total",
		Help: "Kafka writes that failed and were retried.",
	})

	vmEventsDroppedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vm_events_dropped_total",
		Help: "VM events dropped because Kafka could never accept them.",
	})

	vmEventsFilteredTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vm_events_filtered_total",
		Help: "VM watch events not published, by reason.",
	}, []string{"reason", "namespace"})

	vmEventQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "vm_event_queue_depth",
		Help: "VM events queued for the next Kafka write.",
	})

	vmLastEventUnixSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vm_last_event_unix_seconds",
		Help: "Unix time of the most recent VM event acknowledged by Kafka.",
	}, []string{"namespace", "type"})

	vmEventPublishLag = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "vm_event_publish_lag_seconds",
		Help:    "Time from the informer delivering a VM change to Kafka acknowledging it.",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	})
)

// VMEvent is the message value published to Kafka. uid + resourceVersion
// identify the object version; eventFingerprint lets consumers drop the
// duplicates that at-least-once delivery produces.
type VMEvent struct {
	Type             EventType                `json:"type"`
	Namespace        string                   `json:"namespace"`
	Name             string                   `json:"name"`
	UID              string                   `json:"uid"`
	ResourceVersion  string                   `json:"resourceVersion"`
	Generation       int64                    `json:"generation"`
	RunStrategy      string                   `json:"runStrategy,omitempty"`
	PrintableStatus  string                   `json:"status,omitempty"`
	EventFingerprint string                   `json:"eventFingerprint"`
	Timestamp        time.Time                `json:"timestamp"`
	Labels           map[string]string        `json:"labels,omitempty"`
	Annotations      map[string]string        `json:"annotations,omitempty"`
	OwnerReferences  []map[string]interface{} `json:"ownerReferences,omitempty"`
	Spec             json.RawMessage          `json:"spec,omitempty"`
	StatusObject     json.RawMessage          `json:"statusObject,omitempty"`
	Disks            json.RawMessage          `json:"disks,omitempty"`
}

// queuedEvent is a VM change waiting to be written to Kafka.
type queuedEvent struct {
	msg             kafka.Message // key namespace/name, value VMEvent, time observed
	eventType       EventType
	namespace       string
	resourceVersion string
	status          string
}

type config struct {
	WatchAll            bool
	Namespaces          []string
	KafkaBrokers        []string
	KafkaTopic          string
	BatchMaxSize        int
	RetryInitialBackoff time.Duration
	RetryMaxBackoff     time.Duration
	ResyncPeriod        time.Duration
	HTTPAddr            string
}

// messageWriter is the part of *kafka.Writer the watcher uses.
type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := loadConfig()
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}

	restCfg, err := buildConfig()
	if err != nil {
		log.Error("kube config", "err", err)
		os.Exit(1)
	}
	dyn, err := dynamic.NewForConfig(rest.AddUserAgent(restCfg, "vm-watcher"))
	if err != nil {
		log.Error("dynamic client", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	writer := newWriter(cfg, log)

	var ready atomic.Bool
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: httpMux(&ready)}
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

	// Informer handlers queue events here and the publisher writes them. When
	// the queue is full a handler waits, and the informer buffers further
	// notifications in memory rather than dropping them.
	events := make(chan queuedEvent, cfg.BatchMaxSize)

	// Queued events are still written for up to drainTimeout after shutdown starts.
	pubCtx, cancelPub := context.WithCancel(context.Background())
	defer cancelPub()
	context.AfterFunc(ctx, func() { time.AfterFunc(drainTimeout, cancelPub) })

	pub := &publisher{
		log:          log,
		writer:       writer,
		batchMax:     cfg.BatchMaxSize,
		retryInitial: cfg.RetryInitialBackoff,
		retryMax:     cfg.RetryMaxBackoff,
	}
	published := make(chan struct{})
	go func() {
		defer close(published)
		pub.run(pubCtx, events)
	}()

	ctrl := &controller{events: events, done: ctx.Done(), log: log, now: time.Now}
	factories, synced, err := startInformers(ctx, dyn, cfg, ctrl.handlers())
	if err != nil {
		log.Error("start informers", "err", err)
		os.Exit(1)
	}

	log.Info("vm watcher started",
		"namespaces", namespacesLabel(cfg),
		"gvr", vmGVR.String(),
		"brokers", strings.Join(cfg.KafkaBrokers, ","),
		"topic", cfg.KafkaTopic,
		"batchMaxSize", cfg.BatchMaxSize,
		"httpAddr", cfg.HTTPAddr)

	if cache.WaitForCacheSync(ctx.Done(), synced...) {
		ready.Store(true)
		log.Info("caches synced; publishing VM changes")
	}

	<-ctx.Done()
	log.Info("shutting down")
	ready.Store(false)
	// Once the informers have stopped, no handler can queue another event.
	for _, f := range factories {
		f.Shutdown()
	}
	close(events)
	<-published
	if err := writer.Close(); err != nil {
		log.Error("close kafka writer", "err", err)
	}
}

func newWriter(cfg config, log *slog.Logger) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaBrokers...),
		Topic:    cfg.KafkaTopic,
		Balancer: &kafka.Hash{},
		// A whole batch fits in one produce request per partition.
		BatchSize: cfg.BatchMaxSize,
		// How long a partial batch waits for more messages before it is sent.
		BatchTimeout: 5 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
		// Keep the broker connection open between changes. VM changes often
		// come minutes apart, and with kafka-go's 30s default the first one
		// after a pause had to look up the broker in DNS, connect and negotiate
		// versions before it was written: 0.5-5 s on a busy kind node instead
		// of about 10 ms.
		Transport: &kafka.Transport{IdleTimeout: kafkaIdleTimeout},
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			log.Error("kafka: " + fmt.Sprintf(msg, args...))
		}),
	}
}

// startInformers starts one informer per watched namespace, or a single
// cluster-wide one (which needs cluster-scoped list/watch RBAC).
func startInformers(ctx context.Context, dyn dynamic.Interface, cfg config, handler cache.ResourceEventHandler) ([]dynamicinformer.DynamicSharedInformerFactory, []cache.InformerSynced, error) {
	namespaces := cfg.Namespaces
	if cfg.WatchAll {
		namespaces = []string{metav1.NamespaceAll}
	}
	var factories []dynamicinformer.DynamicSharedInformerFactory
	var synced []cache.InformerSynced
	for _, ns := range namespaces {
		f := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dyn, cfg.ResyncPeriod, ns, nil)
		inf := f.ForResource(vmGVR).Informer()
		if _, err := inf.AddEventHandler(handler); err != nil {
			return nil, nil, fmt.Errorf("add handler for namespace %q: %w", ns, err)
		}
		f.Start(ctx.Done())
		factories = append(factories, f)
		synced = append(synced, inf.HasSynced)
	}
	return factories, synced, nil
}

// controller turns informer notifications into queued Kafka messages. An
// informer calls its handlers one notification at a time, so each VM's
// changes are queued in the order the watch delivered them.
type controller struct {
	events chan<- queuedEvent
	done   <-chan struct{}
	log    *slog.Logger
	now    func() time.Time
}

func (c *controller) handlers() cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueue(EventAdded, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldU, okOld := oldObj.(*unstructured.Unstructured)
			newU, okNew := newObj.(*unstructured.Unstructured)
			if !okOld || !okNew {
				c.log.Error("unexpected object type in update handler", "type", fmt.Sprintf("%T", newObj))
				return
			}
			// Informer resyncs re-deliver identical objects; drop them.
			if oldU.GetResourceVersion() == newU.GetResourceVersion() {
				vmEventsFilteredTotal.WithLabelValues("same_resource_version", newU.GetNamespace()).Inc()
				return
			}
			if significantState(oldU) == significantState(newU) {
				vmEventsFilteredTotal.WithLabelValues("insignificant_modified", newU.GetNamespace()).Inc()
				return
			}
			c.enqueue(EventUpdated, newU)
		},
		DeleteFunc: func(obj interface{}) {
			// A delete missed while the watch was down arrives as a tombstone
			// holding the last state the informer saw.
			if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tomb.Obj
			}
			c.enqueue(EventDeleted, obj)
		},
	}
}

// enqueue queues obj for Kafka. It waits while the queue is full, unless the
// watcher is shutting down.
func (c *controller) enqueue(t EventType, obj interface{}) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		c.log.Error("unexpected object type in handler", "type", fmt.Sprintf("%T", obj))
		return
	}
	ev := newVMEvent(t, u, c.now())
	key := ev.Namespace + "/" + ev.Name
	payload, err := json.Marshal(ev)
	if err != nil {
		vmEventsDroppedTotal.Inc()
		c.log.Error("marshal VM event; dropping it", "vm", key, "type", t, "err", err)
		return
	}
	if len(payload) > maxEventBytes {
		vmEventsDroppedTotal.Inc()
		c.log.Error("VM event too large for Kafka; dropping it", "vm", key, "type", t, "bytes", len(payload))
		return
	}

	q := queuedEvent{
		msg:             kafka.Message{Key: []byte(key), Value: payload, Time: ev.Timestamp},
		eventType:       t,
		namespace:       ev.Namespace,
		resourceVersion: ev.ResourceVersion,
		status:          ev.PrintableStatus,
	}
	select {
	case c.events <- q:
		vmEventsObservedTotal.WithLabelValues(string(t), ev.Namespace).Inc()
		vmEventQueueDepth.Set(float64(len(c.events)))
	case <-c.done:
	}
}

func newVMEvent(t EventType, u *unstructured.Unstructured, observed time.Time) VMEvent {
	runStrategy, _, _ := unstructured.NestedString(u.Object, "spec", "runStrategy")
	status, _, _ := unstructured.NestedString(u.Object, "status", "printableStatus")

	var ownerRefs []map[string]interface{}
	for _, owner := range u.GetOwnerReferences() {
		ownerRefs = append(ownerRefs, map[string]interface{}{
			"apiVersion": owner.APIVersion,
			"kind":       owner.Kind,
			"name":       owner.Name,
			"uid":        owner.UID,
			"controller": owner.Controller,
		})
	}

	// Volumes (name, source, size, ...) from spec.template.spec.volumes.
	var disks json.RawMessage
	if volumes, _, _ := unstructured.NestedSlice(u.Object, "spec", "template", "spec", "volumes"); len(volumes) > 0 {
		disks = marshalField(volumes)
	}

	ev := VMEvent{
		Type:            t,
		Namespace:       u.GetNamespace(),
		Name:            u.GetName(),
		UID:             string(u.GetUID()),
		ResourceVersion: u.GetResourceVersion(),
		Generation:      u.GetGeneration(),
		RunStrategy:     runStrategy,
		PrintableStatus: status,
		Timestamp:       observed.UTC(),
		Labels:          u.GetLabels(),
		Annotations:     u.GetAnnotations(),
		OwnerReferences: ownerRefs,
		Spec:            marshalField(u.Object["spec"]),
		StatusObject:    marshalField(u.Object["status"]),
		Disks:           disks,
	}
	ev.EventFingerprint = eventFingerprint(ev)
	return ev
}

// marshalField returns v as JSON, or nil when v is absent.
func marshalField(v interface{}) json.RawMessage {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return b
}

// publisher writes queued VM events to Kafka.
type publisher struct {
	log          *slog.Logger
	writer       messageWriter
	batchMax     int
	retryInitial time.Duration
	retryMax     time.Duration
}

// run writes events batch by batch until the queue is closed and empty, or
// ctx is done.
func (p *publisher) run(ctx context.Context, events <-chan queuedEvent) {
	for {
		batch, ok := nextBatch(events, p.batchMax)
		if !ok {
			return
		}
		vmEventQueueDepth.Set(float64(len(events)))
		if err := p.publish(ctx, batch); err != nil {
			// The next start lists every VM again, so the latest state still
			// reaches Kafka; only intermediate changes are lost.
			p.log.Error("stopped with VM events not written to Kafka",
				"unwritten", len(batch)+len(events), "err", err)
			return
		}
	}
}

// nextBatch waits for one event, then adds whatever is already queued, up to
// limit. It never waits for a batch to fill, so a lone event is not delayed.
// ok is false once events is closed and empty.
func nextBatch(events <-chan queuedEvent, limit int) (batch []queuedEvent, ok bool) {
	ev, ok := <-events
	if !ok {
		return nil, false
	}
	batch = append(batch, ev)
	for len(batch) < limit {
		select {
		case ev, ok := <-events:
			if !ok {
				return batch, true
			}
			batch = append(batch, ev)
		default:
			return batch, true
		}
	}
	return batch, true
}

// publish writes batch, retrying until Kafka has acknowledged every message
// or ctx is done. Nothing newer is written in the meantime, so each VM's
// events stay in order.
func (p *publisher) publish(ctx context.Context, batch []queuedEvent) error {
	delay := p.retryInitial
	for {
		err := p.writer.WriteMessages(ctx, messages(batch)...)
		if err == nil {
			p.recordPublished(batch)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		vmEventsPublishFailuresTotal.Inc()
		batch = p.unwritten(batch, err)
		if len(batch) == 0 {
			return nil
		}
		p.log.Error("write to Kafka failed; retrying", "unwritten", len(batch), "err", err, "retryIn", delay.String())
		if !sleepCtx(ctx, delay) {
			return ctx.Err()
		}
		delay = min(delay*2, p.retryMax)
	}
}

// unwritten returns the events in batch that err says Kafka did not
// acknowledge, and records the rest as published.
func (p *publisher) unwritten(batch []queuedEvent, err error) []queuedEvent {
	var writeErrs kafka.WriteErrors
	if !errors.As(err, &writeErrs) || len(writeErrs) != len(batch) {
		return batch
	}
	var failed, written []queuedEvent
	for i, ev := range batch {
		if writeErrs[i] != nil {
			failed = append(failed, ev)
		} else {
			written = append(written, ev)
		}
	}
	p.recordPublished(written)
	return failed
}

func (p *publisher) recordPublished(batch []queuedEvent) {
	now := time.Now()
	for _, ev := range batch {
		lag := now.Sub(ev.msg.Time)
		vmEventsPublishedTotal.WithLabelValues(string(ev.eventType), ev.namespace).Inc()
		vmLastEventUnixSeconds.WithLabelValues(ev.namespace, string(ev.eventType)).Set(float64(now.Unix()))
		vmEventPublishLag.Observe(lag.Seconds())
		p.log.Info("published vm event",
			"vm", string(ev.msg.Key),
			"type", ev.eventType,
			"status", ev.status,
			"resourceVersion", ev.resourceVersion,
			"lag", lag.String())
	}
}

func messages(batch []queuedEvent) []kafka.Message {
	msgs := make([]kafka.Message, len(batch))
	for i, ev := range batch {
		msgs[i] = ev.msg
	}
	return msgs
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

// buildConfig uses the in-cluster service account, or for local runs the
// usual kubeconfig ($KUBECONFIG, then ~/.kube/config).
func buildConfig() (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{}).ClientConfig()
}

// parseWatchNamespaces supports two modes:
// 1) all namespaces: WATCH_NAMESPACES="*" or "all" (or empty)
// 2) explicit namespaces: WATCH_NAMESPACES="team-a,team-b"
func parseWatchNamespaces(v string) (watchAll bool, namespaces []string) {
	v = strings.TrimSpace(v)
	if v == "" {
		return true, nil
	}
	if v == "*" || strings.EqualFold(v, "all") {
		return true, nil
	}

	seen := map[string]struct{}{}
	for _, ns := range strings.Split(v, ",") {
		ns = strings.TrimSpace(ns)
		if ns == "" {
			continue
		}
		if _, ok := seen[ns]; ok {
			continue
		}
		seen[ns] = struct{}{}
		namespaces = append(namespaces, ns)
	}
	if len(namespaces) == 0 {
		return true, nil
	}
	return false, namespaces
}

func namespacesLabel(cfg config) string {
	if cfg.WatchAll {
		return "*"
	}
	return strings.Join(cfg.Namespaces, ",")
}

// significantState is what a MODIFIED event has to change to be published:
// the spec (metadata.generation), labels, annotations, deletion, printable
// status or Ready condition. Status bookkeeping that KubeVirt controllers
// write often, such as observedGeneration or volume status, is left out.
func significantState(u *unstructured.Unstructured) string {
	printableStatus, _, _ := unstructured.NestedString(u.Object, "status", "printableStatus")
	state, _ := json.Marshal([]interface{}{
		u.GetGeneration(),
		u.GetLabels(),
		u.GetAnnotations(),
		u.GetDeletionTimestamp() != nil,
		printableStatus,
		vmReadyCondition(u),
	})
	return string(state)
}

func vmReadyCondition(u *unstructured.Unstructured) string {
	conds, found, err := unstructured.NestedSlice(u.Object, "status", "conditions")
	if !found || err != nil {
		return ""
	}
	for _, c := range conds {
		m, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		typeV, _, _ := unstructured.NestedString(m, "type")
		if typeV != "Ready" {
			continue
		}
		status, _, _ := unstructured.NestedString(m, "status")
		return status
	}
	return ""
}

func eventFingerprint(ev VMEvent) string {
	raw := strings.Join([]string{
		string(ev.Type),
		ev.Namespace,
		ev.Name,
		ev.UID,
		ev.ResourceVersion,
		fmt.Sprintf("%d", ev.Generation),
		ev.RunStrategy,
		ev.PrintableStatus,
	}, "|")
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func httpMux(ready *atomic.Bool) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	// Ready once the informer caches have synced and changes are flowing.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "informer caches not synced", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

func loadConfig() (config, error) {
	cfg := config{
		KafkaTopic: getenv("KAFKA_TOPIC", "vm-events"),
		HTTPAddr:   getenv("HTTP_ADDR", ":8080"),
	}
	cfg.WatchAll, cfg.Namespaces = parseWatchNamespaces(getenv("WATCH_NAMESPACES", "default"))
	var err error
	if cfg.KafkaBrokers, err = csvEnv("KAFKA_BROKERS"); err != nil {
		return config{}, err
	}
	if cfg.BatchMaxSize, err = intEnv("BATCH_MAX_SIZE", 100); err != nil {
		return config{}, err
	}
	if cfg.RetryInitialBackoff, err = durationEnv("RETRY_INITIAL_BACKOFF", 200*time.Millisecond); err != nil {
		return config{}, err
	}
	if cfg.RetryMaxBackoff, err = durationEnv("RETRY_MAX_BACKOFF", 10*time.Second); err != nil {
		return config{}, err
	}
	if cfg.ResyncPeriod, err = durationEnv("RESYNC_PERIOD", 0); err != nil {
		return config{}, err
	}

	switch {
	case cfg.BatchMaxSize < 1:
		return config{}, errors.New("BATCH_MAX_SIZE must be at least 1")
	case cfg.RetryInitialBackoff <= 0 || cfg.RetryMaxBackoff < cfg.RetryInitialBackoff:
		return config{}, errors.New("RETRY_INITIAL_BACKOFF must be positive and at most RETRY_MAX_BACKOFF")
	case cfg.ResyncPeriod < 0:
		return config{}, errors.New("RESYNC_PERIOD must not be negative (0 turns resyncs off)")
	}
	return cfg, nil
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

func getenv(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}
