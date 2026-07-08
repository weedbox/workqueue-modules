// Package workqueue defines the shared contract implemented by every work
// queue backend in workqueue-modules (memory_workqueue, gorm_workqueue,
// nats_workqueue, ...). Backends register themselves as implementations of
// WorkQueue via github.com/weedbox/weedbox/fxmodule.InterfaceModule, so an
// application can inject the interface and swap or load multiple backends
// side by side.
package workqueue

import (
	"context"
	"errors"
	"time"
)

// Task is the unit of work flowing through a queue. It is shared by all
// backends so callers can switch backends without changing their code.
type Task struct {
	// ID is the unique task identifier (UUID), assigned on Enqueue.
	ID string `json:"id"`

	// Queue is the name of the queue the task belongs to.
	Queue string `json:"queue"`

	// Payload is the opaque task body. Encoding is up to the application.
	Payload []byte `json:"payload"`

	// Metadata carries optional application-defined key/value pairs.
	Metadata map[string]string `json:"metadata,omitempty"`

	// Attempts is the number of times a handler has been invoked for this
	// task so far. It is 0 for a task that has never been delivered; inside
	// a handler it counts the current invocation (first delivery == 1).
	Attempts int `json:"attempts"`

	// MaxRetries is the number of retries allowed after the first failed
	// attempt. A task is moved to the dead-letter queue when a handler
	// fails and Attempts > MaxRetries (i.e. it is invoked at most
	// MaxRetries+1 times).
	MaxRetries int `json:"max_retries"`

	// EnqueuedAt is when the task was originally enqueued.
	EnqueuedAt time.Time `json:"enqueued_at"`

	// RunAt is the earliest time the task may be delivered to a handler.
	RunAt time.Time `json:"run_at"`

	// LastError holds the most recent handler error message. Populated on
	// tasks read back from the dead-letter queue.
	LastError string `json:"last_error,omitempty"`

	// DiedAt is when the task entered the dead-letter queue. Zero for live
	// tasks.
	DiedAt time.Time `json:"died_at,omitzero"`
}

// Handler processes a single task. Returning nil acknowledges the task;
// returning an error schedules a retry with backoff, or moves the task to
// the dead-letter queue once MaxRetries is exhausted.
type Handler func(ctx context.Context, task *Task) error

// Subscription is a handle to a running consumer returned by Consume.
type Subscription interface {
	// Stop gracefully stops the consumer, waiting for in-flight handlers to
	// finish or ctx to expire, whichever comes first. Idempotent.
	Stop(ctx context.Context) error

	// Done is closed once the subscription has fully stopped.
	Done() <-chan struct{}
}

// WorkQueue is the backend-agnostic work queue interface. It covers the
// operations every backend can implement cleanly. Backend-specific features
// live on the concrete type and are reached via named injection or a type
// assertion.
type WorkQueue interface {
	// Enqueue adds a task carrying payload to the named queue and returns
	// the stored task (with its assigned ID). Delivery time, retry budget,
	// and metadata are controlled via EnqueueOption.
	Enqueue(ctx context.Context, queue string, payload []byte, opts ...EnqueueOption) (*Task, error)

	// Consume starts delivering tasks from the named queue to handler and
	// returns immediately with a Subscription controlling the worker pool.
	// Multiple Consume calls (across processes or within one) compete for
	// tasks; each task is delivered to one consumer at a time.
	Consume(ctx context.Context, queue string, handler Handler, opts ...ConsumeOption) (Subscription, error)

	// ListDeadTasks returns tasks that exhausted their retries on the named
	// queue, ordered by the time they died (oldest first).
	ListDeadTasks(ctx context.Context, queue string, limit, offset int) ([]*Task, error)

	// RequeueDeadTask moves a dead task back to the pending queue with its
	// attempt counter reset. Returns ErrTaskNotFound if no dead task with
	// the given ID exists on the queue.
	RequeueDeadTask(ctx context.Context, queue string, taskID string) error

	// DeleteDeadTask permanently removes a dead task. Returns
	// ErrTaskNotFound if no dead task with the given ID exists on the queue.
	DeleteDeadTask(ctx context.Context, queue string, taskID string) error
}

// ErrTaskNotFound is returned by RequeueDeadTask / DeleteDeadTask when the
// referenced task does not exist in the queue's dead-letter storage.
var ErrTaskNotFound = errors.New("workqueue: task not found")
