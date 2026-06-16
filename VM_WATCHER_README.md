# vm-watcher

Watches KubeVirt / OpenShift Virtualization `VirtualMachine` resources in a
configured set of namespaces and publishes **create / update / delete** events
to a message queue (Kafka by default).

Edge-triggered event forwarder — each watch event is transformed into a compact
`VMEvent` and published. Designed as an event/audit feed, **not** a state-convergence
operator. See [Design notes](#design-notes) for why.

---

## What it does

- Watches `kubevirt.io/v1` `VirtualMachine` objects via namespace-scoped informers.
- Emits one event per change: `ADDED`, `MODIFIED`, `DELETED`.
- Publishes to Kafka keyed on `namespace/name` so per-VM event ordering is preserved.
- Handles informer tombstones (missed deletes) and drops resync duplicates.
- Rate-limited retry with a bounded requeue so a flapping broker never blocks the watch.

---

## Architecture

```
                 ┌──────────────────────────────┐
  K8s API <──────┤ namespace-scoped informers    │   (one factory per namespace)
                 │  add / update / delete handlers│
                 └───────────────┬───────────────┘
                                 │ VMEvent
                                 ▼
                       ┌──────────────────┐
                       │ rate-limited      │
                       │ workqueue         │
                       └─────────┬────────┘
                                 │
                      ┌──────────┴──────────┐
                      ▼                     ▼
                  worker 1               worker 2     → Publisher (Kafka)
```

The `Publisher` interface decouples the queue. Kafka is the default
implementation; RabbitMQ / Azure Service Bus / Pub-Sub can be dropped in by
implementing `Publish` + `Close` without touching the watch logic.

---

## Configuration

All configuration is via environment variables.

| Variable | Default | Description |
|---|---|---|
| `WATCH_NAMESPACES` | `default` | Comma-separated namespaces to watch |
| `KAFKA_BROKERS` | `kafka:9092` | Comma-separated broker addresses |
| `KAFKA_TOPIC` | `vm-events` | Destination topic |
| `RESYNC_PERIOD` | `10m` | Informer resync interval |

---

## Event payload

```json
{
  "type": "MODIFIED",
  "namespace": "team-a",
  "name": "web-vm-01",
  "uid": "a1b2c3d4-...",
  "resourceVersion": "184523",
  "generation": 4,
  "runStrategy": "Always",
  "status": "Running",
  "timestamp": "2026-06-16T10:04:00Z"
}
```

`uid` + `resourceVersion` are included so consumers can **dedupe** — delivery is
at-least-once (see [Delivery guarantees](#delivery-guarantees)).

---

## Build & run

### Local (uses `~/.kube/config`)

```bash
go mod tidy
WATCH_NAMESPACES=team-a,team-b KAFKA_BROKERS=localhost:9092 go run .
```

### Container

```bash
docker build -t registry.example.com/vm-watcher:latest .
docker push registry.example.com/vm-watcher:latest
```

### Deploy

```bash
kubectl apply -f deploy/manifests.yaml
```

Edit `deploy/manifests.yaml` first:
- Set `WATCH_NAMESPACES` and `KAFKA_BROKERS`.
- Duplicate the `Role` + `RoleBinding` block **once per watched namespace**.

---

## RBAC

Deliberately **namespace-scoped** — the service only does namespaced LIST/WATCH,
so no `ClusterRole` is required (least privilege). One `Role` + `RoleBinding`
per watched namespace, all bound to a single `ServiceAccount` in the
`vm-watcher` namespace.

```
get, list, watch  on  kubevirt.io/virtualmachines
```

Add `virtualmachineinstances` to the resource list if you switch to watching
runtime (pod-level) lifecycle instead of the VM spec.

---

## Design notes

### Watch-based forwarder vs operator pattern

This service is intentionally an **edge-triggered event forwarder**, not a
controller-runtime operator. The two solve different problems:

| Dimension | Watch-Based Forwarder (this) | Operator Pattern |
|---|---|---|
| What it does | Forwards each VM create/update/delete to the queue | Reconciles VM state toward a desired state |
| Best fit for | Event stream / audit feed / notifications | Owning & acting on a resource's lifecycle |
| Event fidelity | Preserves exact create/update/delete + every transition | Collapses events; can silently coalesce rapid changes |
| Delete guarantees | Best-effort; an event can be lost on a crash | Can guarantee action before deletion (via finalizer) |
| Self-healing on restart | Re-lists; replays existing VMs as "added" | Re-lists and converges to correct state |
| High availability | Needs leader election added (single replica today) | Built-in leader election |
| Build / maintenance effort | Low — small single-purpose service | Higher — heavier framework, more moving parts |
| Operational footprint | Minimal (one binary, small image) | Larger (manager, scheme, webhooks) |
| Delivery semantics | At-least-once; consumers dedupe | At-least-once; convergence-based |

**Why watch-based is the right fit here:** the goal is to forward a faithful
stream of VM events to a queue. An operator's reconcile loop is level-triggered
and coalescing — it would discard the create-vs-update distinction and can drop
rapid intermediate changes, which works against an event feed.

**Switch to an operator only if** one of these becomes a requirement:
- Guaranteed handling on delete (needs a finalizer).
- The service starts **mutating** VMs rather than just observing them.
- Consumers want "current truth about VM X" rather than the sequence of changes.

### Delivery guarantees

Delivery is **at-least-once**, not exactly-once:
- A watch event can be lost if the process crashes between receiving it and publishing.
- The bounded retry can exhaust and drop an event under sustained broker outage.

Consumers must dedupe using `uid` + `resourceVersion`. Exactly-once is a
consumer-side concern, not something this service (or a standard operator) provides.

### High availability

Runs at **`replicas: 1`** by design — multiple replicas would publish every
event multiple times. For HA, add leader election
(`k8s.io/client-go/tools/leaderelection`) so only the leader runs the workers.

### Restart behaviour

On startup the informer LISTs all existing VMs and fires `ADDED` for each. If
consumers can't tolerate replays after a restart, filter on `creationTimestamp`
age or persist the last-seen `resourceVersion` per `uid` and dedupe.

### Filtering status noise

KubeVirt updates `status` frequently. To react only to spec changes, compare
`generation` in the update handler instead of `resourceVersion`.

---

## Health

`GET /healthz` on `:8080` — used by the liveness and readiness probes.
