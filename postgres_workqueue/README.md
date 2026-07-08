# postgres_workqueue

PostgreSQL-native implementation of [`workqueue.WorkQueue`](../workqueue). It shares [`gorm_workqueue`](../gorm_workqueue)'s table layout but exploits PostgreSQL-only features for lower latency and less database pressure:

- **`FOR UPDATE SKIP LOCKED` claims** — one round-trip `UPDATE ... RETURNING` per claim with zero lock contention, however many worker processes compete. No optimistic-lock retry loop.
- **`LISTEN`/`NOTIFY` wakeups** — enqueues `pg_notify` inside the insert transaction, so idle workers in every process wake the moment a task is committed. The poll interval degrades into a slow safety net (default 30s) instead of driving delivery latency.
- **Lease-based crash recovery** — tasks abandoned by a crashed worker return to the pending state once their lease expires, same as `gorm_workqueue`.

## When to use this instead of gorm_workqueue

| | `gorm_workqueue` | `postgres_workqueue` |
|---|---|---|
| Databases | any GORM dialect (PostgreSQL, SQLite, ...) | PostgreSQL only |
| Cross-process pickup latency | poll interval (default 1s) | milliseconds (NOTIFY) |
| Idle DB load | one query per worker per poll tick | near zero (30s safety-net poll) |
| Claim under contention | optimistic UPDATE race, losers retry | `SKIP LOCKED`, no losers |

Both use the same column layout, so you can start on `gorm_workqueue` and switch modules later without migrating data (index names differ; both sets can coexist on one table).

## Installation

```bash
go get github.com/weedbox/workqueue-modules/postgres_workqueue
```

## Usage in Fx

```go
import (
    "github.com/weedbox/common-modules/postgres_connector"
    "github.com/weedbox/workqueue-modules/postgres_workqueue"
)

func loadModules() ([]fx.Option, error) {
    return []fx.Option{
        postgres_connector.Module("database"),
        postgres_workqueue.Module("workqueue"),
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

The dedicated LISTEN connection's DSN is extracted automatically from the GORM postgres dialector; no extra configuration is needed.

## Configuration

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `{scope}.table_name` | `workqueue_tasks` | Table tasks are stored in |
| `{scope}.poll_interval` | `30s` | Safety-net poll for idle workers; delivery normally rides on NOTIFY |
| `{scope}.lease_duration` | `5m` | How long one delivery attempt may run before the task is considered abandoned and redelivered |
| `{scope}.auto_migrate` | `true` | Run AutoMigrate for the task table on start |
| `{scope}.notify_channel` | `<table_name>_wakeup` | LISTEN/NOTIFY channel name |
| `{scope}.listen_dsn` | (from GORM dialector) | Override DSN for the dedicated LISTEN connection |

### TOML Example

```toml
[workqueue]
table_name = "workqueue_tasks"
poll_interval = "30s"
lease_duration = "5m"
auto_migrate = true
```

### Environment Variables

Environment variables require the app prefix passed to `configs.NewConfig()` — `MYAPP` shown here.

```bash
export MYAPP_WORKQUEUE_TABLE_NAME=workqueue_tasks
export MYAPP_WORKQUEUE_POLL_INTERVAL=30s
export MYAPP_WORKQUEUE_LEASE_DURATION=5m
export MYAPP_WORKQUEUE_AUTO_MIGRATE=true
```

## Direct Use (without Fx)

```go
q := postgres_workqueue.New(db, logger, postgres_workqueue.Config{})
defer q.Close() // stops the LISTEN connection

if err := q.AutoMigrate(); err != nil {
    return err
}

task, _ := q.Enqueue(ctx, "emails", payload, workqueue.WithDelay(30*time.Second))

sub, _ := q.Consume(ctx, "emails", func(ctx context.Context, task *workqueue.Task) error {
    return send(task.Payload)
}, workqueue.WithConcurrency(4))

defer sub.Stop(context.Background())
```

## How the wakeup path works

1. `Enqueue` inserts the task and calls `pg_notify(channel, queue)` **in the same transaction**, so listeners only wake once the row is committed and visible.
2. Each process holds one dedicated LISTEN connection (outside GORM's pool — LISTEN is session-scoped). Notifications fan out to that process's idle workers for the named queue.
3. If the LISTEN connection drops, it reconnects with backoff and wakes all queues on reconnect (notifications sent while disconnected are lost); the safety-net poll bounds the worst case.
4. Delayed tasks and retry backoffs don't need NOTIFY: idle workers compute the next `run_at` in the queue and sleep exactly until then (capped by the poll interval).

## Semantics & Caveats

- **At-least-once delivery.** A handler running longer than `lease_duration` may see its task delivered a second time. Size `lease_duration` above your slowest handler, or make handlers idempotent.
- **Completed tasks are deleted.** Successful tasks are removed from the table; only pending/running/dead rows remain.
- **One extra connection per process** for LISTEN. If it cannot be established (no extractable DSN), the backend logs a warning and falls back to pure polling.

## Testing

Tests need a real PostgreSQL and skip themselves otherwise:

```bash
docker run -d --name wq-pg -p 55432:5432 -e POSTGRES_PASSWORD=test postgres:16-alpine
WORKQUEUE_TEST_POSTGRES_DSN="host=localhost port=55432 user=postgres password=test dbname=postgres sslmode=disable" \
    go test ./postgres_workqueue/
docker rm -f wq-pg
```
