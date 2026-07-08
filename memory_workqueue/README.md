# memory_workqueue

In-process, in-memory implementation of [`workqueue.WorkQueue`](../workqueue). Tasks live only for the lifetime of the process, making this backend a drop-in choice for development and tests — the same role `local_storage_connector` plays for storage-modules.

## Features

- Full `workqueue.WorkQueue` semantics: delayed delivery, exponential retry backoff, dead-letter queue, requeue/delete of dead tasks
- Timer-based scheduling (no polling) — delayed tasks wake workers exactly when due
- Safe for concurrent use; multiple `Consume` calls compete for tasks with exactly-once delivery per attempt
- Panic recovery: a panicking handler counts as a failed attempt

## Installation

```bash
go get github.com/weedbox/workqueue-modules/memory_workqueue
```

## Usage in Fx

```go
import "github.com/weedbox/workqueue-modules/memory_workqueue"

func loadModules() ([]fx.Option, error) {
    return []fx.Option{
        memory_workqueue.Module("workqueue"),
    }, nil
}
```

Consumers inject the shared interface:

```go
import "github.com/weedbox/workqueue-modules/workqueue"

type Params struct {
    fx.In
    WorkQueue workqueue.WorkQueue // no name tag needed when single-loaded
}
```

## Direct Use (without Fx)

```go
q := memory_workqueue.New(logger) // nil logger is fine

task, _ := q.Enqueue(ctx, "emails", payload,
    workqueue.WithDelay(30*time.Second),
    workqueue.WithMaxRetries(5),
)

sub, _ := q.Consume(ctx, "emails", func(ctx context.Context, task *workqueue.Task) error {
    return send(task.Payload)
}, workqueue.WithConcurrency(4))

defer sub.Stop(context.Background())
```

## Configuration

This backend has no configuration keys — everything is controlled per call via `EnqueueOption` / `ConsumeOption`.

## Caveats

- **Not durable.** All pending and dead tasks are lost when the process exits. Use [`gorm_workqueue`](../gorm_workqueue) or [`nats_workqueue`](../nats_workqueue) when tasks must survive restarts.
- **Single process.** Consumers in other processes cannot see this queue.
