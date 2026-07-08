// Package workqueuetest provides a reusable conformance suite that exercises
// the workqueue.WorkQueue contract. Every backend in this repository runs the
// same suite so retry, delay, and dead-letter semantics stay uniform across
// implementations.
package workqueuetest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weedbox/workqueue-modules/workqueue"
)

// Factory returns a fresh, started WorkQueue backed by isolated storage.
// Implementations should register cleanup via t.Cleanup.
type Factory func(t *testing.T) workqueue.WorkQueue

const (
	waitTimeout = 15 * time.Second
	pollEvery   = 10 * time.Millisecond
)

var queueSeq atomic.Int64

// nextQueue returns a unique queue name safe for every backend (no dots or
// wildcard characters, so it is valid as a NATS subject token).
func nextQueue() string {
	return fmt.Sprintf("q%d", queueSeq.Add(1))
}

// fastBackoff keeps retry delays short so the suite runs quickly.
func fastBackoff() workqueue.ConsumeOption {
	return workqueue.WithRetryBackoff(20*time.Millisecond, 100*time.Millisecond)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(pollEvery)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func stop(t *testing.T, sub workqueue.Subscription) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	if err := sub.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// Run executes the full conformance suite against queues produced by factory.
func Run(t *testing.T, factory Factory) {
	t.Run("EnqueueAndConsume", func(t *testing.T) { testEnqueueAndConsume(t, factory) })
	t.Run("DelayedDelivery", func(t *testing.T) { testDelayedDelivery(t, factory) })
	t.Run("RetryThenSucceed", func(t *testing.T) { testRetryThenSucceed(t, factory) })
	t.Run("RetriesExhaustedGoToDeadLetter", func(t *testing.T) { testRetriesExhausted(t, factory) })
	t.Run("ZeroMaxRetriesDiesAfterFirstFailure", func(t *testing.T) { testZeroMaxRetries(t, factory) })
	t.Run("RequeueDeadTask", func(t *testing.T) { testRequeueDeadTask(t, factory) })
	t.Run("DeleteDeadTask", func(t *testing.T) { testDeleteDeadTask(t, factory) })
	t.Run("ListDeadTasksPagination", func(t *testing.T) { testListDeadTasksPagination(t, factory) })
	t.Run("CompetingConsumersDeliverOnce", func(t *testing.T) { testCompetingConsumers(t, factory) })
	t.Run("QueuesAreIsolated", func(t *testing.T) { testQueuesAreIsolated(t, factory) })
	t.Run("StopWaitsForInFlightHandler", func(t *testing.T) { testStopWaitsForInFlight(t, factory) })
}

func testEnqueueAndConsume(t *testing.T, factory Factory) {
	q := factory(t)
	queue := nextQueue()
	ctx := context.Background()

	enq, err := q.Enqueue(ctx, queue, []byte("hello"),
		workqueue.WithMetadata(map[string]string{"trace": "abc"}))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if enq.ID == "" {
		t.Fatal("Enqueue returned task without ID")
	}
	if enq.Queue != queue {
		t.Fatalf("task.Queue = %q, want %q", enq.Queue, queue)
	}
	if enq.MaxRetries != workqueue.DefaultMaxRetries {
		t.Fatalf("task.MaxRetries = %d, want default %d", enq.MaxRetries, workqueue.DefaultMaxRetries)
	}

	var mu sync.Mutex
	var got *workqueue.Task

	sub, err := q.Consume(ctx, queue, func(ctx context.Context, task *workqueue.Task) error {
		mu.Lock()
		defer mu.Unlock()
		got = task
		return nil
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer stop(t, sub)

	waitFor(t, "task delivery", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return got != nil
	})

	mu.Lock()
	defer mu.Unlock()
	if got.ID != enq.ID {
		t.Fatalf("delivered task ID = %q, want %q", got.ID, enq.ID)
	}
	if string(got.Payload) != "hello" {
		t.Fatalf("payload = %q, want %q", got.Payload, "hello")
	}
	if got.Metadata["trace"] != "abc" {
		t.Fatalf("metadata = %v, want trace=abc", got.Metadata)
	}
	if got.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", got.Attempts)
	}
}

func testDelayedDelivery(t *testing.T, factory Factory) {
	q := factory(t)
	queue := nextQueue()
	ctx := context.Background()

	const delay = time.Second

	var delivered atomic.Int64
	var deliveredAt atomic.Value // time.Time

	sub, err := q.Consume(ctx, queue, func(ctx context.Context, task *workqueue.Task) error {
		deliveredAt.Store(time.Now())
		delivered.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer stop(t, sub)

	start := time.Now()
	if _, err := q.Enqueue(ctx, queue, []byte("later"), workqueue.WithDelay(delay)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Well before the delay elapses, nothing may have been delivered.
	time.Sleep(delay / 3)
	if n := delivered.Load(); n != 0 {
		t.Fatalf("task delivered %d time(s) before its delay elapsed", n)
	}

	waitFor(t, "delayed delivery", func() bool { return delivered.Load() == 1 })

	at := deliveredAt.Load().(time.Time)
	// Allow modest scheduling slack, but the delivery must not fire early.
	if elapsed := at.Sub(start); elapsed < delay-50*time.Millisecond {
		t.Fatalf("task delivered after %v, want >= %v", elapsed, delay)
	}
}

func testRetryThenSucceed(t *testing.T, factory Factory) {
	q := factory(t)
	queue := nextQueue()
	ctx := context.Background()

	var mu sync.Mutex
	var attemptsSeen []int

	sub, err := q.Consume(ctx, queue, func(ctx context.Context, task *workqueue.Task) error {
		mu.Lock()
		attemptsSeen = append(attemptsSeen, task.Attempts)
		n := len(attemptsSeen)
		mu.Unlock()
		if n < 3 {
			return errors.New("transient failure")
		}
		return nil
	}, fastBackoff())
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer stop(t, sub)

	if _, err := q.Enqueue(ctx, queue, []byte("flaky")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	waitFor(t, "3 handler invocations", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attemptsSeen) >= 3
	})

	// Give a moment for any (incorrect) extra redelivery to show up.
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(attemptsSeen) != 3 {
		t.Fatalf("handler invoked %d times, want exactly 3", len(attemptsSeen))
	}
	for i, a := range attemptsSeen {
		if a != i+1 {
			t.Fatalf("attemptsSeen = %v, want [1 2 3]", attemptsSeen)
		}
	}

	dead, err := q.ListDeadTasks(ctx, queue, 10, 0)
	if err != nil {
		t.Fatalf("ListDeadTasks: %v", err)
	}
	if len(dead) != 0 {
		t.Fatalf("dead tasks = %d, want 0", len(dead))
	}
}

func testRetriesExhausted(t *testing.T, factory Factory) {
	q := factory(t)
	queue := nextQueue()
	ctx := context.Background()

	var invocations atomic.Int64

	sub, err := q.Consume(ctx, queue, func(ctx context.Context, task *workqueue.Task) error {
		invocations.Add(1)
		return errors.New("permanent failure")
	}, fastBackoff())
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer stop(t, sub)

	enq, err := q.Enqueue(ctx, queue, []byte("doomed"), workqueue.WithMaxRetries(1))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	var dead []*workqueue.Task
	waitFor(t, "task in dead-letter queue", func() bool {
		var err error
		dead, err = q.ListDeadTasks(ctx, queue, 10, 0)
		if err != nil {
			t.Fatalf("ListDeadTasks: %v", err)
		}
		return len(dead) == 1
	})

	// MaxRetries=1 → exactly 2 invocations.
	time.Sleep(300 * time.Millisecond)
	if n := invocations.Load(); n != 2 {
		t.Fatalf("handler invoked %d times, want exactly 2", n)
	}

	d := dead[0]
	if d.ID != enq.ID {
		t.Fatalf("dead task ID = %q, want %q", d.ID, enq.ID)
	}
	if string(d.Payload) != "doomed" {
		t.Fatalf("dead payload = %q, want %q", d.Payload, "doomed")
	}
	if d.LastError == "" {
		t.Fatal("dead task LastError is empty")
	}
	if d.DiedAt.IsZero() {
		t.Fatal("dead task DiedAt is zero")
	}
	if d.Attempts != 2 {
		t.Fatalf("dead task Attempts = %d, want 2", d.Attempts)
	}
}

func testZeroMaxRetries(t *testing.T, factory Factory) {
	q := factory(t)
	queue := nextQueue()
	ctx := context.Background()

	var invocations atomic.Int64

	sub, err := q.Consume(ctx, queue, func(ctx context.Context, task *workqueue.Task) error {
		invocations.Add(1)
		return errors.New("fails once")
	}, fastBackoff())
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer stop(t, sub)

	if _, err := q.Enqueue(ctx, queue, []byte("one-shot"), workqueue.WithMaxRetries(0)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	waitFor(t, "task in dead-letter queue", func() bool {
		dead, err := q.ListDeadTasks(ctx, queue, 10, 0)
		if err != nil {
			t.Fatalf("ListDeadTasks: %v", err)
		}
		return len(dead) == 1
	})

	time.Sleep(300 * time.Millisecond)
	if n := invocations.Load(); n != 1 {
		t.Fatalf("handler invoked %d times, want exactly 1", n)
	}
}

func testRequeueDeadTask(t *testing.T, factory Factory) {
	q := factory(t)
	queue := nextQueue()
	ctx := context.Background()

	var fail atomic.Bool
	fail.Store(true)

	var mu sync.Mutex
	var successAttempts []int

	sub, err := q.Consume(ctx, queue, func(ctx context.Context, task *workqueue.Task) error {
		if fail.Load() {
			return errors.New("failing phase")
		}
		mu.Lock()
		successAttempts = append(successAttempts, task.Attempts)
		mu.Unlock()
		return nil
	}, fastBackoff())
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer stop(t, sub)

	enq, err := q.Enqueue(ctx, queue, []byte("revive-me"), workqueue.WithMaxRetries(0))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	waitFor(t, "task in dead-letter queue", func() bool {
		dead, err := q.ListDeadTasks(ctx, queue, 10, 0)
		if err != nil {
			t.Fatalf("ListDeadTasks: %v", err)
		}
		return len(dead) == 1
	})

	// Requeue of an unknown ID must fail with ErrTaskNotFound.
	if err := q.RequeueDeadTask(ctx, queue, "no-such-task"); !errors.Is(err, workqueue.ErrTaskNotFound) {
		t.Fatalf("RequeueDeadTask(unknown) = %v, want ErrTaskNotFound", err)
	}

	fail.Store(false)
	if err := q.RequeueDeadTask(ctx, queue, enq.ID); err != nil {
		t.Fatalf("RequeueDeadTask: %v", err)
	}

	waitFor(t, "requeued task success", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(successAttempts) == 1
	})

	mu.Lock()
	if successAttempts[0] != 1 {
		t.Fatalf("requeued task Attempts = %d, want 1 (counter reset)", successAttempts[0])
	}
	mu.Unlock()

	dead, err := q.ListDeadTasks(ctx, queue, 10, 0)
	if err != nil {
		t.Fatalf("ListDeadTasks: %v", err)
	}
	if len(dead) != 0 {
		t.Fatalf("dead tasks after requeue = %d, want 0", len(dead))
	}
}

func testDeleteDeadTask(t *testing.T, factory Factory) {
	q := factory(t)
	queue := nextQueue()
	ctx := context.Background()

	sub, err := q.Consume(ctx, queue, func(ctx context.Context, task *workqueue.Task) error {
		return errors.New("always fails")
	}, fastBackoff())
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer stop(t, sub)

	enq, err := q.Enqueue(ctx, queue, []byte("condemned"), workqueue.WithMaxRetries(0))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	waitFor(t, "task in dead-letter queue", func() bool {
		dead, err := q.ListDeadTasks(ctx, queue, 10, 0)
		if err != nil {
			t.Fatalf("ListDeadTasks: %v", err)
		}
		return len(dead) == 1
	})

	if err := q.DeleteDeadTask(ctx, queue, "no-such-task"); !errors.Is(err, workqueue.ErrTaskNotFound) {
		t.Fatalf("DeleteDeadTask(unknown) = %v, want ErrTaskNotFound", err)
	}

	if err := q.DeleteDeadTask(ctx, queue, enq.ID); err != nil {
		t.Fatalf("DeleteDeadTask: %v", err)
	}

	dead, err := q.ListDeadTasks(ctx, queue, 10, 0)
	if err != nil {
		t.Fatalf("ListDeadTasks: %v", err)
	}
	if len(dead) != 0 {
		t.Fatalf("dead tasks after delete = %d, want 0", len(dead))
	}

	// Deleting again must report not found.
	if err := q.DeleteDeadTask(ctx, queue, enq.ID); !errors.Is(err, workqueue.ErrTaskNotFound) {
		t.Fatalf("DeleteDeadTask(again) = %v, want ErrTaskNotFound", err)
	}
}

func testListDeadTasksPagination(t *testing.T, factory Factory) {
	q := factory(t)
	queue := nextQueue()
	ctx := context.Background()

	sub, err := q.Consume(ctx, queue, func(ctx context.Context, task *workqueue.Task) error {
		return errors.New("always fails")
	}, fastBackoff(), workqueue.WithConcurrency(1))
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer stop(t, sub)

	// Stagger the enqueues so death order is deterministic (concurrency=1
	// keeps processing order aligned with enqueue order).
	var ids []string
	for i := range 3 {
		enq, err := q.Enqueue(ctx, queue, fmt.Appendf(nil, "dead-%d", i), workqueue.WithMaxRetries(0))
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		ids = append(ids, enq.ID)

		waitFor(t, fmt.Sprintf("%d dead task(s)", i+1), func() bool {
			dead, err := q.ListDeadTasks(ctx, queue, 10, 0)
			if err != nil {
				t.Fatalf("ListDeadTasks: %v", err)
			}
			return len(dead) == i+1
		})
	}

	all, err := q.ListDeadTasks(ctx, queue, 10, 0)
	if err != nil {
		t.Fatalf("ListDeadTasks: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("dead tasks = %d, want 3", len(all))
	}
	for i, d := range all {
		if d.ID != ids[i] {
			t.Fatalf("dead[%d].ID = %q, want %q (oldest first)", i, d.ID, ids[i])
		}
	}

	page, err := q.ListDeadTasks(ctx, queue, 1, 1)
	if err != nil {
		t.Fatalf("ListDeadTasks(limit=1, offset=1): %v", err)
	}
	if len(page) != 1 || page[0].ID != ids[1] {
		t.Fatalf("page = %+v, want single task %q", page, ids[1])
	}

	empty, err := q.ListDeadTasks(ctx, queue, 10, 5)
	if err != nil {
		t.Fatalf("ListDeadTasks(offset beyond end): %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("out-of-range page = %d tasks, want 0", len(empty))
	}
}

func testCompetingConsumers(t *testing.T, factory Factory) {
	q := factory(t)
	queue := nextQueue()
	ctx := context.Background()

	const total = 20

	var mu sync.Mutex
	seen := make(map[string]int)

	handler := func(ctx context.Context, task *workqueue.Task) error {
		mu.Lock()
		seen[task.ID]++
		mu.Unlock()
		return nil
	}

	sub1, err := q.Consume(ctx, queue, handler, workqueue.WithConcurrency(4))
	if err != nil {
		t.Fatalf("Consume #1: %v", err)
	}
	defer stop(t, sub1)

	sub2, err := q.Consume(ctx, queue, handler, workqueue.WithConcurrency(4))
	if err != nil {
		t.Fatalf("Consume #2: %v", err)
	}
	defer stop(t, sub2)

	for i := range total {
		if _, err := q.Enqueue(ctx, queue, fmt.Appendf(nil, "job-%d", i)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	waitFor(t, "all tasks processed", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == total
	})

	// Let any duplicate delivery surface before asserting.
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("task %s delivered %d times, want 1", id, n)
		}
	}
}

func testQueuesAreIsolated(t *testing.T, factory Factory) {
	q := factory(t)
	queueA, queueB := nextQueue(), nextQueue()
	ctx := context.Background()

	var deliveredA, deliveredB atomic.Int64

	subA, err := q.Consume(ctx, queueA, func(ctx context.Context, task *workqueue.Task) error {
		deliveredA.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("Consume A: %v", err)
	}
	defer stop(t, subA)

	subB, err := q.Consume(ctx, queueB, func(ctx context.Context, task *workqueue.Task) error {
		deliveredB.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("Consume B: %v", err)
	}
	defer stop(t, subB)

	if _, err := q.Enqueue(ctx, queueA, []byte("for-A")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	waitFor(t, "delivery on queue A", func() bool { return deliveredA.Load() == 1 })

	time.Sleep(300 * time.Millisecond)
	if n := deliveredB.Load(); n != 0 {
		t.Fatalf("queue B received %d task(s), want 0", n)
	}
}

func testStopWaitsForInFlight(t *testing.T, factory Factory) {
	q := factory(t)
	queue := nextQueue()
	ctx := context.Background()

	started := make(chan struct{})
	var finished atomic.Bool

	sub, err := q.Consume(ctx, queue, func(ctx context.Context, task *workqueue.Task) error {
		close(started)
		time.Sleep(500 * time.Millisecond)
		finished.Store(true)
		return nil
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	if _, err := q.Enqueue(ctx, queue, []byte("slow")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case <-started:
	case <-time.After(waitTimeout):
		t.Fatal("timed out waiting for handler to start")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	if err := sub.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if !finished.Load() {
		t.Fatal("Stop returned before the in-flight handler finished")
	}

	select {
	case <-sub.Done():
	default:
		t.Fatal("Done() not closed after Stop returned")
	}
}
