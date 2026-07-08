package workqueue

import (
	"maps"
	"time"
)

// Defaults shared by all backends.
const (
	// DefaultMaxRetries is the retry budget applied when WithMaxRetries is
	// not given: a failing task is retried 3 times (4 invocations total)
	// before moving to the dead-letter queue.
	DefaultMaxRetries = 3

	// DefaultConcurrency is the number of concurrent handler invocations a
	// subscription runs when WithConcurrency is not given.
	DefaultConcurrency = 10

	// DefaultBackoffBase / DefaultBackoffMax bound the exponential retry
	// backoff applied when WithRetryBackoff is not given.
	DefaultBackoffBase = 10 * time.Second
	DefaultBackoffMax  = 10 * time.Minute
)

// EnqueueOptions collects the per-task settings accepted by Enqueue.
type EnqueueOptions struct {
	// RunAt is the earliest delivery time. Zero means "now".
	RunAt time.Time

	// Delay postpones delivery relative to now. Ignored when RunAt is set.
	Delay time.Duration

	// MaxRetries overrides DefaultMaxRetries. Negative values are treated
	// as 0 (no retries).
	MaxRetries int

	// Metadata carries application-defined key/value pairs on the task.
	Metadata map[string]string
}

// EnqueueOption mutates EnqueueOptions.
type EnqueueOption func(*EnqueueOptions)

// NewEnqueueOptions applies opts on top of the defaults and returns the
// resolved settings. Backends call this once per Enqueue.
func NewEnqueueOptions(opts ...EnqueueOption) *EnqueueOptions {
	o := &EnqueueOptions{
		MaxRetries: DefaultMaxRetries,
	}
	for _, opt := range opts {
		opt(o)
	}
	if o.MaxRetries < 0 {
		o.MaxRetries = 0
	}
	return o
}

// ResolveRunAt computes the effective earliest delivery time relative to now.
func (o *EnqueueOptions) ResolveRunAt(now time.Time) time.Time {
	if !o.RunAt.IsZero() {
		return o.RunAt
	}
	if o.Delay > 0 {
		return now.Add(o.Delay)
	}
	return now
}

// WithDelay postpones the task's first delivery by d.
func WithDelay(d time.Duration) EnqueueOption {
	return func(o *EnqueueOptions) { o.Delay = d }
}

// WithRunAt schedules the task's first delivery at t. Takes precedence over
// WithDelay.
func WithRunAt(t time.Time) EnqueueOption {
	return func(o *EnqueueOptions) { o.RunAt = t }
}

// WithMaxRetries overrides the task's retry budget (retries after the first
// failed attempt; the handler runs at most n+1 times).
func WithMaxRetries(n int) EnqueueOption {
	return func(o *EnqueueOptions) { o.MaxRetries = n }
}

// WithMetadata attaches application-defined key/value pairs to the task.
// Later calls merge over earlier ones.
func WithMetadata(md map[string]string) EnqueueOption {
	return func(o *EnqueueOptions) {
		if o.Metadata == nil {
			o.Metadata = make(map[string]string, len(md))
		}
		maps.Copy(o.Metadata, md)
	}
}

// ConsumeOptions collects the per-subscription settings accepted by Consume.
type ConsumeOptions struct {
	// Concurrency is the max number of handler invocations in flight.
	Concurrency int

	// BackoffBase / BackoffMax bound the exponential retry backoff: the
	// n-th retry waits min(BackoffBase * 2^(n-1), BackoffMax).
	BackoffBase time.Duration
	BackoffMax  time.Duration
}

// ConsumeOption mutates ConsumeOptions.
type ConsumeOption func(*ConsumeOptions)

// NewConsumeOptions applies opts on top of the defaults and returns the
// resolved settings. Backends call this once per Consume.
func NewConsumeOptions(opts ...ConsumeOption) *ConsumeOptions {
	o := &ConsumeOptions{
		Concurrency: DefaultConcurrency,
		BackoffBase: DefaultBackoffBase,
		BackoffMax:  DefaultBackoffMax,
	}
	for _, opt := range opts {
		opt(o)
	}
	if o.Concurrency < 1 {
		o.Concurrency = 1
	}
	if o.BackoffBase <= 0 {
		o.BackoffBase = DefaultBackoffBase
	}
	if o.BackoffMax < o.BackoffBase {
		o.BackoffMax = o.BackoffBase
	}
	return o
}

// WithConcurrency sets the max number of concurrent handler invocations for
// the subscription.
func WithConcurrency(n int) ConsumeOption {
	return func(o *ConsumeOptions) { o.Concurrency = n }
}

// WithRetryBackoff bounds the exponential retry backoff: the n-th retry
// waits min(base * 2^(n-1), max).
func WithRetryBackoff(base, max time.Duration) ConsumeOption {
	return func(o *ConsumeOptions) {
		o.BackoffBase = base
		o.BackoffMax = max
	}
}

// RetryBackoff returns the delay before the attempt-th retry (attempt >= 1):
// min(base * 2^(attempt-1), max). Backends share this so retry timing is
// uniform across implementations.
func RetryBackoff(base, max time.Duration, attempt int) time.Duration {
	if base <= 0 {
		base = DefaultBackoffBase
	}
	if max < base {
		max = base
	}
	if attempt < 1 {
		attempt = 1
	}

	d := base
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= max || d < 0 { // d < 0 guards overflow
			return max
		}
	}
	if d > max {
		return max
	}
	return d
}
