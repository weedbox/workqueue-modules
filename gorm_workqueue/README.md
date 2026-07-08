# gorm_workqueue

Database-backed implementation of [`workqueue.WorkQueue`](../workqueue) on top of any `database.DatabaseConnector` from common-modules (PostgreSQL, SQLite, ...). Tasks are durable rows; consumers poll for ready tasks and claim them with an optimistic status transition, so multiple processes can safely compete for the same queue.

## Features

- Durable tasks — pending, retrying, and dead tasks survive process restarts
- Multi-process safe: claims are optimistic `UPDATE ... WHERE status = 'pending' AND attempts = ?` transitions, so competing workers never double-deliver an attempt
- Lease-based crash recovery: tasks abandoned by a crashed worker return to the pending state once their lease expires
- Full `workqueue.WorkQueue` semantics: delayed delivery, exponential retry backoff, dead-letter state, requeue/delete of dead tasks
- Local enqueues wake idle workers immediately, and idle workers sleep exactly until the next scheduled task comes due; the poll interval only bounds pickup of tasks enqueued by other processes

## Installation

```bash
go get github.com/weedbox/workqueue-modules/gorm_workqueue
```

## Usage in Fx

```go
import (
    "github.com/weedbox/common-modules/postgres_connector"
    "github.com/weedbox/workqueue-modules/gorm_workqueue"
)

func loadModules() ([]fx.Option, error) {
    return []fx.Option{
        postgres_connector.Module("database"), // or sqlite_connector
        gorm_workqueue.Module("workqueue"),
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
| `{scope}.table_name` | `workqueue_tasks` | Table tasks are stored in |
| `{scope}.poll_interval` | `1s` | How often idle workers look for ready tasks |
| `{scope}.lease_duration` | `5m` | How long one delivery attempt may run before the task is considered abandoned and redelivered |
| `{scope}.auto_migrate` | `true` | Run AutoMigrate for the task table on start |

### TOML Example

```toml
[workqueue]
table_name = "workqueue_tasks"
poll_interval = "1s"
lease_duration = "5m"
auto_migrate = true
```

### Environment Variables

Environment variables require the app prefix passed to `configs.NewConfig()` — `MYAPP` shown here.

```bash
export MYAPP_WORKQUEUE_TABLE_NAME=workqueue_tasks
export MYAPP_WORKQUEUE_POLL_INTERVAL=1s
export MYAPP_WORKQUEUE_LEASE_DURATION=5m
export MYAPP_WORKQUEUE_AUTO_MIGRATE=true
```

## Direct Use (without Fx)

```go
q := gorm_workqueue.New(db, logger, gorm_workqueue.Config{
    PollInterval:  time.Second,
    LeaseDuration: 5 * time.Minute,
})
if err := q.AutoMigrate(); err != nil {
    return err
}

task, _ := q.Enqueue(ctx, "emails", payload, workqueue.WithDelay(30*time.Second))

sub, _ := q.Consume(ctx, "emails", func(ctx context.Context, task *workqueue.Task) error {
    return send(task.Payload)
}, workqueue.WithConcurrency(4))

defer sub.Stop(context.Background())
```

## Semantics & Caveats

- **At-least-once delivery.** A handler running longer than `lease_duration` may see its task delivered a second time. Size `lease_duration` above your slowest handler, or make handlers idempotent.
- **Completed tasks are deleted.** Successful tasks are removed from the table; only pending/running/dead rows remain.
- **Cross-process pickup latency** is bounded by `poll_interval`; tasks scheduled locally (delays, retries) fire on time regardless. On PostgreSQL, [`postgres_workqueue`](../postgres_workqueue) removes this bound via LISTEN/NOTIFY.
