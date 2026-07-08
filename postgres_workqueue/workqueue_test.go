package postgres_workqueue

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/weedbox/workqueue-modules/workqueue"
	"github.com/weedbox/workqueue-modules/workqueuetest"
)

const testTable = "workqueue_tasks_conformance"

// testDSN returns the PostgreSQL DSN for tests, skipping the test when none
// is configured. Example:
//
//	WORKQUEUE_TEST_POSTGRES_DSN="host=localhost port=5432 user=postgres password=test dbname=postgres sslmode=disable" go test ./postgres_workqueue/
func testDSN(t *testing.T) string {
	t.Helper()

	dsn := os.Getenv("WORKQUEUE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set WORKQUEUE_TEST_POSTGRES_DSN to run postgres_workqueue tests")
	}
	return dsn
}

func newTestDB(t *testing.T, dsn string) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}

	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			sqlDB.Close()
		}
	})

	return db
}

// dropOnce clears leftovers from previous test runs exactly once per test
// process; queue names are only unique within one run.
var dropOnce sync.Once

func newTestQueueWithConfig(t *testing.T, config Config) *PostgresWorkQueue {
	t.Helper()

	db := newTestDB(t, testDSN(t))

	dropOnce.Do(func() {
		if err := db.Exec("DROP TABLE IF EXISTS " + testTable).Error; err != nil {
			t.Fatalf("drop test table: %v", err)
		}
	})

	config.TableName = testTable
	q := New(db, zap.NewNop(), config)
	t.Cleanup(q.Close)

	if err := q.AutoMigrate(); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	return q
}

func newTestQueue(t *testing.T) *PostgresWorkQueue {
	// The long poll interval is deliberate: conformance passing with it
	// proves delivery is driven by LISTEN/NOTIFY wakeups and next-run-at
	// waits, not by the poll safety net.
	return newTestQueueWithConfig(t, Config{
		PollInterval:  5 * time.Second,
		LeaseDuration: 30 * time.Second,
	})
}

func TestConformance(t *testing.T) {
	workqueuetest.Run(t, func(t *testing.T) workqueue.WorkQueue {
		return newTestQueue(t)
	})
}

// TestCrossProcessNotify verifies that a task enqueued through one queue
// instance is picked up by a consumer on another instance (separate GORM pool
// and LISTEN connection, as in another process) far faster than the poll
// interval allows.
func TestCrossProcessNotify(t *testing.T) {
	consumer := newTestQueueWithConfig(t, Config{
		PollInterval:  30 * time.Second,
		LeaseDuration: 30 * time.Second,
	})
	producer := newTestQueueWithConfig(t, Config{
		PollInterval:  30 * time.Second,
		LeaseDuration: 30 * time.Second,
	})

	ctx := context.Background()

	delivered := make(chan struct{})
	sub, err := consumer.Consume(ctx, "cross_process_notify", func(ctx context.Context, task *workqueue.Task) error {
		close(delivered)
		return nil
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer sub.Stop(context.Background())

	// Give the consumer time to go idle waiting on its 30s poll timer.
	time.Sleep(200 * time.Millisecond)

	start := time.Now()
	if _, err := producer.Enqueue(ctx, "cross_process_notify", []byte("ping")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case <-delivered:
		if elapsed := time.Since(start); elapsed > 3*time.Second {
			t.Fatalf("cross-instance delivery took %v, want well under the 30s poll interval", elapsed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("task enqueued on another instance was never delivered (LISTEN/NOTIFY path broken)")
	}
}

func TestExpiredLeaseIsRedelivered(t *testing.T) {
	q := newTestQueueWithConfig(t, Config{
		PollInterval:  100 * time.Millisecond,
		LeaseDuration: 300 * time.Millisecond,
	})

	ctx := context.Background()

	enq, err := q.Enqueue(ctx, "pg_leases", []byte("abandoned"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Claim the task directly (simulating a worker) and then never complete
	// it, as if the process crashed.
	record, _, err := q.claim(ctx, "pg_leases")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if record == nil || record.ID != enq.ID {
		t.Fatalf("claim returned %+v, want task %s", record, enq.ID)
	}

	var delivered atomic.Int64
	sub, err := q.Consume(ctx, "pg_leases", func(ctx context.Context, task *workqueue.Task) error {
		if task.ID == enq.ID {
			delivered.Add(1)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer sub.Stop(context.Background())

	deadline := time.Now().Add(10 * time.Second)
	for delivered.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if delivered.Load() == 0 {
		t.Fatal("abandoned task was not redelivered after its lease expired")
	}
}

func TestNotStartedReturnsError(t *testing.T) {
	q := &PostgresWorkQueue{logger: zap.NewNop(), notifies: map[string]chan struct{}{}}

	if _, err := q.Enqueue(context.Background(), "x", nil); err != errNotStarted {
		t.Fatalf("Enqueue = %v, want errNotStarted", err)
	}
	if _, err := q.Consume(context.Background(), "x", nil); err != errNotStarted {
		t.Fatalf("Consume = %v, want errNotStarted", err)
	}
}
