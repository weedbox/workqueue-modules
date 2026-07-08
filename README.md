# workqueue-modules

Task queue modules for the [Weedbox](https://github.com/weedbox/weedbox) ecosystem. A single `workqueue.WorkQueue` interface with interchangeable backends, so application code stays backend-agnostic: enqueue and consume tasks, delay/schedule delivery, retry with exponential backoff, and inspect/requeue/delete dead-lettered tasks.

## Packages

| Package | Description |
|---------|-------------|
| [`workqueue`](./workqueue) | Shared `WorkQueue` interface, `Task` model, enqueue/consume options, backoff helper |
| [`memory_workqueue`](./memory_workqueue) | In-process, in-memory backend for development and tests |
| [`gorm_workqueue`](./gorm_workqueue) | Database-backed backend on any `database.DatabaseConnector` (PostgreSQL, SQLite, ...); polling with optimistic claims and lease-based crash recovery |
| [`postgres_workqueue`](./postgres_workqueue) | PostgreSQL-native backend; `FOR UPDATE SKIP LOCKED` claims and LISTEN/NOTIFY wakeups, same table layout as `gorm_workqueue` |
| [`nats_workqueue`](./nats_workqueue) | NATS JetStream backend; push-based delivery with per-queue task and dead-letter streams |
| [`workqueuetest`](./workqueuetest) | Conformance test suite every backend must pass |

## The Interface

```go
type WorkQueue interface {
    Enqueue(ctx context.Context, queue string, payload []byte, opts ...EnqueueOption) (*Task, error)
    Consume(ctx context.Context, queue string, handler Handler, opts ...ConsumeOption) (Subscription, error)

    ListDeadTasks(ctx context.Context, queue string, limit, offset int) ([]*Task, error)
    RequeueDeadTask(ctx context.Context, queue string, taskID string) error
    DeleteDeadTask(ctx context.Context, queue string, taskID string) error
}
```

Semantics shared by all backends:

- **Attempts** — `Task.Attempts` is the number of times the handler has been invoked for the task (first delivery = 1). A handler panic counts as a failed attempt.
- **Retries** — a failed task is retried with exponential backoff (`WithRetryBackoff`, default 10s base / 10m cap) until it has failed `MaxRetries + 1` times, then it moves to the dead-letter queue with `LastError` and `DiedAt` recorded.
- **Delay/scheduling** — `WithDelay(d)` or `WithRunAt(t)` defers delivery until the task is due.
- **Dead letters** — `ListDeadTasks` returns oldest-first with limit/offset paging; `RequeueDeadTask` resets the attempt counter and re-enqueues; unknown task IDs return `ErrTaskNotFound`.
- **Delivery** — at-least-once. Competing consumers on the same queue never double-deliver an attempt under normal operation, but crash recovery may redeliver; make handlers idempotent.
- **Shutdown** — `Subscription.Stop` waits for in-flight handlers to finish; `Done()` closes once fully stopped.

## Usage

Load one backend module and inject the interface — application code never references a concrete backend:

```go
import (
    "github.com/weedbox/common-modules/postgres_connector"
    "github.com/weedbox/workqueue-modules/gorm_workqueue"
    "github.com/weedbox/workqueue-modules/workqueue"
)

func loadModules() ([]fx.Option, error) {
    return []fx.Option{
        postgres_connector.Module("database"),
        gorm_workqueue.Module("workqueue"), // or memory_workqueue / nats_workqueue
    }, nil
}

type Params struct {
    fx.In
    WorkQueue workqueue.WorkQueue
}
```

Enqueue and consume:

```go
task, err := p.WorkQueue.Enqueue(ctx, "emails", payload,
    workqueue.WithDelay(30*time.Second),
    workqueue.WithMaxRetries(5),
    workqueue.WithMetadata(map[string]string{"tenant": "acme"}),
)

sub, err := p.WorkQueue.Consume(ctx, "emails",
    func(ctx context.Context, task *workqueue.Task) error {
        return send(task.Payload)
    },
    workqueue.WithConcurrency(4),
    workqueue.WithRetryBackoff(10*time.Second, 10*time.Minute),
)
defer sub.Stop(context.Background())
```

Multiple backends can also be loaded side by side; inject each by its scope name:

```go
type Params struct {
    fx.In
    Jobs   workqueue.WorkQueue `name:"jobs"`   // gorm_workqueue.Module("jobs")
    Events workqueue.WorkQueue `name:"events"` // nats_workqueue.Module("events")
}
```

## Choosing a Backend

| | `memory_workqueue` | `gorm_workqueue` | `postgres_workqueue` | `nats_workqueue` |
|---|---|---|---|---|
| Durability | none (process-local) | database rows | database rows | replicated JetStream streams |
| Multi-process | no | yes (optimistic claims) | yes (`SKIP LOCKED`) | yes (shared durable consumer) |
| Delivery latency | immediate | poll interval (cross-process) | push (LISTEN/NOTIFY) | push, immediate |
| Extra infra | none | existing database (any dialect) | existing PostgreSQL | NATS with JetStream |
| Best for | tests, development | SQLite or mixed-dialect apps, modest volume | apps on PostgreSQL | high-volume or low-latency pipelines |

## Testing

Every backend passes the shared conformance suite in [`workqueuetest`](./workqueuetest):

```go
func TestConformance(t *testing.T) {
    workqueuetest.Run(t, func(t *testing.T) workqueue.WorkQueue {
        return newMyBackend(t)
    })
}
```

Run everything:

```bash
go test ./...
```
