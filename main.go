// vm-watcher: watches KubeVirt VirtualMachine resources in specific namespaces
// and publishes create/update/delete events to Kafka.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
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

var (
	vmEventsObservedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vm_events_observed_total",
		Help: "Total number of VM watch events observed by the informer.",
	}, []string{"type", "namespace"})

	vmEventsPublishedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vm_events_published_total",
		Help: "Total number of VM events successfully published to the sink.",
	}, []string{"type", "namespace", "name"})

	vmEventsPublishFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vm_events_publish_failures_total",
		Help: "Total number of VM events that failed to publish.",
	}, []string{"type", "namespace", "name"})

	vmEventQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "vm_event_queue_depth",
		Help: "Current number of queued VM events waiting to be published.",
	})

	vmLastEventUnixSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vm_last_event_unix_seconds",
		Help: "Unix timestamp of the most recent published VM event.",
	}, []string{"namespace", "name", "type"})
)

// VMEvent is the wire format published to the queue.
// uid + resourceVersion let consumers dedupe (at-least-once delivery).
type VMEvent struct {
	Type            EventType `json:"type"`
	Namespace       string    `json:"namespace"`
	Name            string    `json:"name"`
	UID             string    `json:"uid"`
	ResourceVersion string    `json:"resourceVersion"`
	Generation      int64     `json:"generation"`
	RunStrategy     string    `json:"runStrategy,omitempty"`
	PrintableStatus string    `json:"status,omitempty"`
	Timestamp       time.Time `json:"timestamp"`
}

// Publisher abstracts the message queue so Kafka can be swapped for
// RabbitMQ / Azure Service Bus / Pub-Sub without touching watch logic.
type Publisher interface {
	Publish(ctx context.Context, key string, payload []byte) error
	Close() error
}

type kafkaPublisher struct{ w *kafka.Writer }

func newKafkaPublisher(brokers []string, topic string) *kafkaPublisher {
	return &kafkaPublisher{w: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // key = namespace/name -> per-VM ordering
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 50 * time.Millisecond,
	}}
}

func (p *kafkaPublisher) Publish(ctx context.Context, key string, payload []byte) error {
	return p.w.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: payload})
}
func (p *kafkaPublisher) Close() error { return p.w.Close() }

type postgresPublisher struct{ pool *pgxpool.Pool }

func newPostgresPublisher(ctx context.Context, dsn string) (*postgresPublisher, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS vm_events (
			id               BIGSERIAL    PRIMARY KEY,
			event_key        TEXT         NOT NULL,
			payload          JSONB        NOT NULL,
			created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
		)`)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("create table: %w", err)
	}
	return &postgresPublisher{pool: pool}, nil
}

func (p *postgresPublisher) Publish(ctx context.Context, key string, payload []byte) error {
	_, err := p.pool.Exec(ctx,
		`INSERT INTO vm_events (event_key, payload) VALUES ($1, $2)`,
		key, payload)
	return err
}
func (p *postgresPublisher) Close() error { p.pool.Close(); return nil }

type controller struct {
	queue     workqueue.RateLimitingInterface
	publisher Publisher
	log       *slog.Logger
}

func (c *controller) handlers() cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueue(EventAdded, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldU, newU := oldObj.(*unstructured.Unstructured), newObj.(*unstructured.Unstructured)
			// Informer resyncs re-deliver identical objects; drop them.
			if oldU.GetResourceVersion() == newU.GetResourceVersion() {
				return
			}
			c.enqueue(EventUpdated, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			// Handle tombstones from missed delete watch events.
			if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tomb.Obj
			}
			c.enqueue(EventDeleted, obj)
		},
	}
}

func (c *controller) enqueue(t EventType, obj interface{}) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		c.log.Error("unexpected object type in handler", "type", fmt.Sprintf("%T", obj))
		return
	}
	runStrategy, _, _ := unstructured.NestedString(u.Object, "spec", "runStrategy")
	status, _, _ := unstructured.NestedString(u.Object, "status", "printableStatus")
	vmEventsObservedTotal.WithLabelValues(string(t), u.GetNamespace()).Inc()
	c.queue.Add(VMEvent{
		Type:            t,
		Namespace:       u.GetNamespace(),
		Name:            u.GetName(),
		UID:             string(u.GetUID()),
		ResourceVersion: u.GetResourceVersion(),
		Generation:      u.GetGeneration(),
		RunStrategy:     runStrategy,
		PrintableStatus: status,
		Timestamp:       time.Now().UTC(),
	})
	vmEventQueueDepth.Set(float64(c.queue.Len()))
}

// worker drains the queue and publishes; rate-limited retries on broker errors
// so a flapping broker never blocks the informers.
func (c *controller) runWorker(ctx context.Context) {
	for {
		item, shutdown := c.queue.Get()
		if shutdown {
			return
		}
		ev, ok := item.(VMEvent)
		if !ok {
			c.log.Error("unexpected queue item type", "type", fmt.Sprintf("%T", item))
			c.queue.Done(item)
			c.queue.Forget(item)
			vmEventQueueDepth.Set(float64(c.queue.Len()))
			continue
		}
		func() {
			defer c.queue.Done(ev)
			payload, err := json.Marshal(ev)
			if err != nil {
				c.log.Error("marshal failed, dropping", "err", err)
				vmEventsPublishFailuresTotal.WithLabelValues(string(ev.Type), ev.Namespace, ev.Name).Inc()
				c.queue.Forget(ev)
				vmEventQueueDepth.Set(float64(c.queue.Len()))
				return
			}
			pubCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			if err := c.publisher.Publish(pubCtx, ev.Namespace+"/"+ev.Name, payload); err != nil {
				c.log.Warn("publish failed, requeueing",
					"vm", ev.Namespace+"/"+ev.Name, "type", ev.Type,
					"retries", c.queue.NumRequeues(ev), "err", err)
				vmEventsPublishFailuresTotal.WithLabelValues(string(ev.Type), ev.Namespace, ev.Name).Inc()
				if c.queue.NumRequeues(ev) < 12 {
					c.queue.AddRateLimited(ev)
					vmEventQueueDepth.Set(float64(c.queue.Len()))
					return
				}
				c.log.Error("max retries exceeded, dropping event", "vm", ev.Namespace+"/"+ev.Name)
			} else {
				vmEventsPublishedTotal.WithLabelValues(string(ev.Type), ev.Namespace, ev.Name).Inc()
				vmLastEventUnixSeconds.WithLabelValues(ev.Namespace, ev.Name, string(ev.Type)).Set(float64(time.Now().UTC().Unix()))
				c.log.Info("published vm event",
					"vm", ev.Namespace+"/"+ev.Name,
					"type", ev.Type,
					"generation", ev.Generation,
					"resourceVersion", ev.ResourceVersion)
			}
			c.queue.Forget(ev)
			vmEventQueueDepth.Set(float64(c.queue.Len()))
		}()
	}
}

func buildConfig() (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	// Local dev fallback
	return clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
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

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	watchAllNamespaces, namespaces := parseWatchNamespaces(getenv("WATCH_NAMESPACES", "default"))
	brokers := strings.Split(getenv("KAFKA_BROKERS", "kafka:9092"), ",")
	topic := getenv("KAFKA_TOPIC", "vm-events")
	resync, _ := time.ParseDuration(getenv("RESYNC_PERIOD", "10m"))

	cfg, err := buildConfig()
	if err != nil {
		log.Error("kube config", "err", err)
		os.Exit(1)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Error("dynamic client", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	sinkType := getenv("SINK_TYPE", "kafka")
	var pub Publisher
	switch sinkType {
	case "postgres":
		dsn := getenv("POSTGRES_DSN", "postgres://vmwatcher:changeme@postgres:5432/vmwatcher?sslmode=disable")
		pgPub, err := newPostgresPublisher(ctx, dsn)
		if err != nil {
			log.Error("postgres publisher init failed", "err", err)
			os.Exit(1)
		}
		pub = pgPub
		log.Info("sink: postgres")
	default:
		pub = newKafkaPublisher(brokers, topic)
		log.Info("sink: kafka", "brokers", brokers, "topic", topic)
	}

	ctrl := &controller{
		queue: workqueue.NewRateLimitingQueue(
			workqueue.DefaultControllerRateLimiter()),
		publisher: pub,
		log:       log,
	}
	defer ctrl.publisher.Close()

	// One filtered factory per namespace in explicit mode; one cluster-wide
	// factory in all-namespace mode.
	// Note: all-namespace mode requires cluster-scoped LIST/WATCH RBAC.
	var synced []cache.InformerSynced
	if watchAllNamespaces {
		f := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dyn, resync, metav1.NamespaceAll, nil)
		inf := f.ForResource(vmGVR).Informer()
		if _, err := inf.AddEventHandler(ctrl.handlers()); err != nil {
			log.Error("add handler", "scope", "all-namespaces", "err", err)
			os.Exit(1)
		}
		f.Start(ctx.Done())
		synced = append(synced, inf.HasSynced)
		log.Info("watching", "scope", "all-namespaces", "gvr", vmGVR.String())
	} else {
		for _, ns := range namespaces {
			f := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dyn, resync, ns, nil)
			inf := f.ForResource(vmGVR).Informer()
			if _, err := inf.AddEventHandler(ctrl.handlers()); err != nil {
				log.Error("add handler", "namespace", ns, "err", err)
				os.Exit(1)
			}
			f.Start(ctx.Done())
			synced = append(synced, inf.HasSynced)
			log.Info("watching", "namespace", ns, "gvr", vmGVR.String())
		}
	}

	if !cache.WaitForCacheSync(ctx.Done(), synced...) {
		log.Error("cache sync failed")
		os.Exit(1)
	}
	log.Info("caches synced, starting workers")

	go ctrl.runWorker(ctx)
	go ctrl.runWorker(ctx)

	// liveness/readiness and Prometheus metrics
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: ":8080", Handler: mux}
	go srv.ListenAndServe()

	<-ctx.Done()
	log.Info("shutting down")
	ctrl.queue.ShutDown()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
