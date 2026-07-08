# nats_workqueue

NATS JetStream implementation of [`workqueue.WorkQueue`](../workqueue). Each queue maps to a work-queue-retention stream plus a browsable dead-letter stream, so tasks are durable and any number of processes can compete for the same queue with push-based (no polling) delivery.

## Features

- Durable, replicated tasks (JetStream file storage; replica count configurable)
- Push-based delivery — no poll interval; tasks are dispatched the moment they are due
- Multi-process safe: one durable consumer per queue, competing subscribers share it
- Full `workqueue.WorkQueue` semantics: delayed delivery (`NakWithDelay` scheduling), exponential retry backoff, dead-letter stream, requeue/delete of dead tasks
- Long-running handlers are kept alive with in-progress heartbeats every `ack_wait / 3`

## How it works

- Tasks are JSON messages on `<subject_prefix>.<queue>.tasks`, held in stream `<stream_prefix>_<queue>` (work-queue retention: a message is removed once acknowledged).
- Retry bookkeeping lives **in the task payload**, not in JetStream delivery counts: a failed attempt republishes the task with an incremented attempt counter and a backoff `RunAt`, then acknowledges the original message (publish-before-ack, so a crash in between duplicates rather than loses).
- A task delivered before its `RunAt` is rescheduled with `NakWithDelay`, which never distorts the attempt count.
- Exhausted tasks are published to `<subject_prefix>.<queue>.dead` in stream `<stream_prefix>_<queue>_DLQ` (limits retention, so they stay browsable until deleted or requeued).

## Installation

```bash
go get github.com/weedbox/workqueue-modules/nats_workqueue
```

## Usage in Fx

```go
import (
    "github.com/weedbox/common-modules/nats_connector"
    "github.com/weedbox/workqueue-modules/nats_workqueue"
)

func loadModules() ([]fx.Option, error) {
    return []fx.Option{
        nats_connector.Module("nats"),
        nats_workqueue.Module("workqueue"),
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

## Configuration

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `{scope}.stream_prefix` | `WORKQUEUE` | Stream name prefix (`<prefix>_<queue>`, `<prefix>_<queue>_DLQ`) |
| `{scope}.subject_prefix` | `workqueue` | Subject prefix (`<prefix>.<queue>.tasks`, `<prefix>.<queue>.dead`) |
| `{scope}.replicas` | `0` | Stream replicas; `0` targets 3 with automatic fallback to 1 on single-node servers |
| `{scope}.ack_wait` | `30s` | Per-delivery acknowledgement deadline; handlers running longer are kept alive by heartbeats |

### TOML Example

```toml
[workqueue]
stream_prefix = "WORKQUEUE"
subject_prefix = "workqueue"
replicas = 0
ack_wait = "30s"
```

### Environment Variables

Environment variables require the app prefix passed to `configs.NewConfig()` — `MYAPP` shown here.

```bash
export MYAPP_WORKQUEUE_STREAM_PREFIX=WORKQUEUE
export MYAPP_WORKQUEUE_SUBJECT_PREFIX=workqueue
export MYAPP_WORKQUEUE_REPLICAS=0
export MYAPP_WORKQUEUE_ACK_WAIT=30s
```

## Direct Use (without Fx)

```go
q, err := nats_workqueue.New(conn, logger, nats_workqueue.Config{})
if err != nil {
    return err
}

task, _ := q.Enqueue(ctx, "emails", payload, workqueue.WithDelay(30*time.Second))

sub, _ := q.Consume(ctx, "emails", func(ctx context.Context, task *workqueue.Task) error {
    return send(task.Payload)
}, workqueue.WithConcurrency(4))

defer sub.Stop(context.Background())
```

## Semantics & Caveats

- **At-least-once delivery.** A crash between handler completion and acknowledgement (or between republish and ack) redelivers or duplicates an attempt. Make handlers idempotent.
- **Queue names are subject tokens.** They must not contain `.`, `*`, `>`, spaces, or slashes; invalid names return `ErrInvalidQueueName`.
- **Consumer settings are shared per queue.** All subscribers of a queue attach to one durable consumer, so `ack_wait` and the in-flight cap (set from the first subscriber's concurrency) apply queue-wide.
- **Dead tasks persist until acted on.** The DLQ stream has no age/size limit by default; list, requeue, or delete dead tasks to reclaim space.
