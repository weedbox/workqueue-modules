package gorm_workqueue

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/weedbox/workqueue-modules/workqueue"
	"github.com/weedbox/workqueue-modules/workqueuetest"
)

var dbSeq atomic.Int64

// newTestDB opens an isolated in-memory SQLite database. A single connection
// serializes concurrent workers the same way the connector's pool would.
func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:gorm_workqueue_test_%d?mode=memory&cache=shared", dbSeq.Add(1))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db handle: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { sqlDB.Close() })

	return db
}

func newTestQueue(t *testing.T) *GormWorkQueue {
	t.Helper()

	// The long poll interval is deliberate: conformance passing with it
	// proves delivery is driven by local wakeups and next-run-at waits, not
	// by the poll safety net.
	q := New(newTestDB(t), zap.NewNop(), Config{
		PollInterval:  2 * time.Second,
		LeaseDuration: 30 * time.Second,
	})
	if err := q.AutoMigrate(); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	return q
}

func TestConformance(t *testing.T) {
	workqueuetest.Run(t, func(t *testing.T) workqueue.WorkQueue {
		return newTestQueue(t)
	})
}

func TestExpiredLeaseIsRedelivered(t *testing.T) {
	db := newTestDB(t)

	q := New(db, zap.NewNop(), Config{
		PollInterval:  20 * time.Millisecond,
		LeaseDuration: 300 * time.Millisecond,
	})
	if err := q.AutoMigrate(); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}

	ctx := context.Background()

	enq, err := q.Enqueue(ctx, "leases", []byte("abandoned"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Claim the task directly (simulating a worker) and then never complete
	// it, as if the process crashed.
	record, _, err := q.claim(ctx, "leases")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if record == nil || record.ID != enq.ID {
		t.Fatalf("claim returned %+v, want task %s", record, enq.ID)
	}

	var delivered atomic.Int64
	sub, err := q.Consume(ctx, "leases", func(ctx context.Context, task *workqueue.Task) error {
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
	q := &GormWorkQueue{logger: zap.NewNop(), notifies: map[string]chan struct{}{}}

	if _, err := q.Enqueue(context.Background(), "x", nil); err != errNotStarted {
		t.Fatalf("Enqueue = %v, want errNotStarted", err)
	}
	if _, err := q.Consume(context.Background(), "x", nil); err != errNotStarted {
		t.Fatalf("Consume = %v, want errNotStarted", err)
	}
}
