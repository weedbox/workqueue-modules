// Package memory_workqueue provides an in-process, in-memory implementation
// of workqueue.WorkQueue. Tasks live only for the lifetime of the process,
// making it a drop-in backend for development and tests (the same role
// local_storage_connector plays for storage-modules).
package memory_workqueue

import (
	"container/heap"
	"context"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/weedbox/weedbox/fxmodule"
	"github.com/weedbox/workqueue-modules/workqueue"
)

// MemoryWorkQueue is an in-memory workqueue.WorkQueue implementation. Safe
// for concurrent use.
type MemoryWorkQueue struct {
	logger *zap.Logger

	mu     sync.Mutex
	queues map[string]*queueState
}

// Compile-time check that MemoryWorkQueue satisfies the shared interface.
var _ workqueue.WorkQueue = (*MemoryWorkQueue)(nil)

type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *zap.Logger
}

// Module registers the in-memory backend as an implementation of
// workqueue.WorkQueue. It can be loaded on its own (inject the interface
// without a name tag) or side by side with other backends (inject with
// name:"<scope>").
func Module(scope string) fx.Option {
	return fxmodule.InterfaceModule[workqueue.WorkQueue](
		scope,
		func(p Params) workqueue.WorkQueue {
			q := New(p.Logger.Named(scope))

			p.Lifecycle.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					q.logger.Info("Starting MemoryWorkQueue")
					return nil
				},
				OnStop: func(ctx context.Context) error {
					q.logger.Info("Stopped MemoryWorkQueue")
					return nil
				},
			})

			return q
		},
	)
}

// New creates a MemoryWorkQueue outside of fx. A nil logger is replaced with
// a no-op logger.
func New(logger *zap.Logger) *MemoryWorkQueue {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &MemoryWorkQueue{
		logger: logger,
		queues: make(map[string]*queueState),
	}
}

// queueState holds one queue's pending heap and dead-letter list. notify is
// closed and replaced whenever the pending set changes, waking sleeping
// workers (broadcast semantics).
type queueState struct {
	pending pendingHeap
	seq     int64
	dead    []*workqueue.Task
	notify  chan struct{}
}

func (m *MemoryWorkQueue) queue(name string) *queueState {
	q, ok := m.queues[name]
	if !ok {
		q = &queueState{notify: make(chan struct{})}
		m.queues[name] = q
	}
	return q
}

// broadcast wakes every worker waiting on the queue.
func (q *queueState) broadcast() {
	close(q.notify)
	q.notify = make(chan struct{})
}

// pendingItem wraps a task with an insertion sequence so equal RunAt values
// dequeue in FIFO order.
type pendingItem struct {
	task *workqueue.Task
	seq  int64
}

type pendingHeap []*pendingItem

func (h pendingHeap) Len() int { return len(h) }
func (h pendingHeap) Less(i, j int) bool {
	if !h[i].task.RunAt.Equal(h[j].task.RunAt) {
		return h[i].task.RunAt.Before(h[j].task.RunAt)
	}
	return h[i].seq < h[j].seq
}
func (h pendingHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *pendingHeap) Push(x any)   { *h = append(*h, x.(*pendingItem)) }
func (h *pendingHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

// cloneTask returns a deep-enough copy so handlers and dead-letter readers
// never share mutable state with the queue's internal record.
func cloneTask(t *workqueue.Task) *workqueue.Task {
	c := *t
	if t.Payload != nil {
		c.Payload = append([]byte(nil), t.Payload...)
	}
	if t.Metadata != nil {
		c.Metadata = maps.Clone(t.Metadata)
	}
	return &c
}

// Enqueue adds a task to the named queue.
func (m *MemoryWorkQueue) Enqueue(ctx context.Context, queue string, payload []byte, opts ...workqueue.EnqueueOption) (*workqueue.Task, error) {

	o := workqueue.NewEnqueueOptions(opts...)
	now := time.Now()

	task := &workqueue.Task{
		ID:         uuid.NewString(),
		Queue:      queue,
		Payload:    append([]byte(nil), payload...),
		Metadata:   maps.Clone(o.Metadata),
		MaxRetries: o.MaxRetries,
		EnqueuedAt: now,
		RunAt:      o.ResolveRunAt(now),
	}

	// Copy before publishing: once the task is on the heap a worker may
	// claim and mutate it concurrently.
	ret := cloneTask(task)

	m.mu.Lock()
	q := m.queue(queue)
	q.seq++
	heap.Push(&q.pending, &pendingItem{task: task, seq: q.seq})
	q.broadcast()
	m.mu.Unlock()

	return ret, nil
}

// claim pops the next ready task from the queue, incrementing its attempt
// counter. When nothing is ready it returns the wait duration until the next
// scheduled task (or a large idle wait) plus the current notify channel.
func (m *MemoryWorkQueue) claim(queue string) (*workqueue.Task, time.Duration, chan struct{}) {

	const idleWait = time.Minute

	m.mu.Lock()
	defer m.mu.Unlock()

	q := m.queue(queue)

	if q.pending.Len() == 0 {
		return nil, idleWait, q.notify
	}

	now := time.Now()
	next := q.pending[0].task
	if next.RunAt.After(now) {
		return nil, next.RunAt.Sub(now), q.notify
	}

	item := heap.Pop(&q.pending).(*pendingItem)
	item.task.Attempts++
	return item.task, 0, nil
}

// complete finalizes one delivery: on failure the task is either rescheduled
// with backoff or moved to the dead-letter list.
func (m *MemoryWorkQueue) complete(queue string, task *workqueue.Task, handlerErr error, o *workqueue.ConsumeOptions) {

	if handlerErr == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	q := m.queue(queue)

	if task.Attempts > task.MaxRetries {
		task.LastError = handlerErr.Error()
		task.DiedAt = time.Now()
		q.dead = append(q.dead, task)

		m.logger.Warn("Task moved to dead-letter queue",
			zap.String("queue", queue),
			zap.String("task_id", task.ID),
			zap.Int("attempts", task.Attempts),
			zap.Error(handlerErr),
		)
		return
	}

	task.RunAt = time.Now().Add(workqueue.RetryBackoff(o.BackoffBase, o.BackoffMax, task.Attempts))
	q.seq++
	heap.Push(&q.pending, &pendingItem{task: task, seq: q.seq})
	q.broadcast()
}

// Consume starts a worker pool delivering tasks from the named queue.
func (m *MemoryWorkQueue) Consume(ctx context.Context, queue string, handler workqueue.Handler, opts ...workqueue.ConsumeOption) (workqueue.Subscription, error) {

	o := workqueue.NewConsumeOptions(opts...)

	sub := newSubscription()

	var wg sync.WaitGroup
	for range o.Concurrency {
		wg.Go(func() {
			m.worker(queue, handler, o, sub.stopCh)
		})
	}

	go func() {
		wg.Wait()
		close(sub.done)
	}()

	return sub, nil
}

func (m *MemoryWorkQueue) worker(queue string, handler workqueue.Handler, o *workqueue.ConsumeOptions, stopCh <-chan struct{}) {

	for {
		select {
		case <-stopCh:
			return
		default:
		}

		task, wait, notify := m.claim(queue)
		if task != nil {
			err := invokeHandler(handler, cloneTask(task))
			m.complete(queue, task, err, o)
			continue
		}

		timer := time.NewTimer(wait)
		select {
		case <-stopCh:
			timer.Stop()
			return
		case <-notify:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// invokeHandler runs the handler with panic recovery; a panic counts as a
// failed attempt.
func invokeHandler(handler workqueue.Handler, task *workqueue.Task) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("workqueue handler panic: %v", r)
		}
	}()
	return handler(context.Background(), task)
}

// ListDeadTasks returns dead tasks ordered oldest first.
func (m *MemoryWorkQueue) ListDeadTasks(ctx context.Context, queue string, limit, offset int) ([]*workqueue.Task, error) {

	m.mu.Lock()
	defer m.mu.Unlock()

	q := m.queue(queue)

	if offset < 0 {
		offset = 0
	}
	if offset >= len(q.dead) {
		return nil, nil
	}

	end := len(q.dead)
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}

	out := make([]*workqueue.Task, 0, end-offset)
	for _, t := range q.dead[offset:end] {
		out = append(out, cloneTask(t))
	}
	return out, nil
}

// RequeueDeadTask moves a dead task back to the pending queue with its
// attempt counter reset.
func (m *MemoryWorkQueue) RequeueDeadTask(ctx context.Context, queue string, taskID string) error {

	m.mu.Lock()
	defer m.mu.Unlock()

	q := m.queue(queue)

	for i, t := range q.dead {
		if t.ID != taskID {
			continue
		}

		q.dead = append(q.dead[:i], q.dead[i+1:]...)

		t.Attempts = 0
		t.LastError = ""
		t.DiedAt = time.Time{}
		t.RunAt = time.Now()

		q.seq++
		heap.Push(&q.pending, &pendingItem{task: t, seq: q.seq})
		q.broadcast()
		return nil
	}

	return workqueue.ErrTaskNotFound
}

// DeleteDeadTask permanently removes a dead task.
func (m *MemoryWorkQueue) DeleteDeadTask(ctx context.Context, queue string, taskID string) error {

	m.mu.Lock()
	defer m.mu.Unlock()

	q := m.queue(queue)

	for i, t := range q.dead {
		if t.ID == taskID {
			q.dead = append(q.dead[:i], q.dead[i+1:]...)
			return nil
		}
	}

	return workqueue.ErrTaskNotFound
}

// subscription implements workqueue.Subscription for the in-memory backend.
type subscription struct {
	stopOnce sync.Once
	stopCh   chan struct{}
	done     chan struct{}
}

func newSubscription() *subscription {
	return &subscription{
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
}

func (s *subscription) Stop(ctx context.Context) error {
	s.stopOnce.Do(func() { close(s.stopCh) })

	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *subscription) Done() <-chan struct{} {
	return s.done
}
