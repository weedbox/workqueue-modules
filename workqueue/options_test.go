package workqueue

import (
	"testing"
	"time"
)

func TestNewEnqueueOptionsDefaults(t *testing.T) {
	o := NewEnqueueOptions()

	if o.MaxRetries != DefaultMaxRetries {
		t.Fatalf("MaxRetries = %d, want %d", o.MaxRetries, DefaultMaxRetries)
	}

	now := time.Now()
	if got := o.ResolveRunAt(now); !got.Equal(now) {
		t.Fatalf("ResolveRunAt = %v, want %v", got, now)
	}
}

func TestNewEnqueueOptionsNegativeMaxRetriesClampsToZero(t *testing.T) {
	o := NewEnqueueOptions(WithMaxRetries(-5))
	if o.MaxRetries != 0 {
		t.Fatalf("MaxRetries = %d, want 0", o.MaxRetries)
	}
}

func TestResolveRunAtDelay(t *testing.T) {
	now := time.Now()

	o := NewEnqueueOptions(WithDelay(time.Minute))
	if got, want := o.ResolveRunAt(now), now.Add(time.Minute); !got.Equal(want) {
		t.Fatalf("ResolveRunAt = %v, want %v", got, want)
	}
}

func TestResolveRunAtRunAtWinsOverDelay(t *testing.T) {
	now := time.Now()
	at := now.Add(3 * time.Hour)

	o := NewEnqueueOptions(WithDelay(time.Minute), WithRunAt(at))
	if got := o.ResolveRunAt(now); !got.Equal(at) {
		t.Fatalf("ResolveRunAt = %v, want %v", got, at)
	}
}

func TestWithMetadataMerges(t *testing.T) {
	o := NewEnqueueOptions(
		WithMetadata(map[string]string{"a": "1", "b": "2"}),
		WithMetadata(map[string]string{"b": "3"}),
	)

	if o.Metadata["a"] != "1" || o.Metadata["b"] != "3" {
		t.Fatalf("Metadata = %v, want a=1 b=3", o.Metadata)
	}
}

func TestNewConsumeOptionsDefaults(t *testing.T) {
	o := NewConsumeOptions()

	if o.Concurrency != DefaultConcurrency {
		t.Fatalf("Concurrency = %d, want %d", o.Concurrency, DefaultConcurrency)
	}
	if o.BackoffBase != DefaultBackoffBase || o.BackoffMax != DefaultBackoffMax {
		t.Fatalf("Backoff = %v/%v, want %v/%v", o.BackoffBase, o.BackoffMax, DefaultBackoffBase, DefaultBackoffMax)
	}
}

func TestNewConsumeOptionsClamps(t *testing.T) {
	o := NewConsumeOptions(WithConcurrency(0), WithRetryBackoff(time.Second, time.Millisecond))

	if o.Concurrency != 1 {
		t.Fatalf("Concurrency = %d, want 1", o.Concurrency)
	}
	if o.BackoffMax != o.BackoffBase {
		t.Fatalf("BackoffMax = %v, want clamped to base %v", o.BackoffMax, o.BackoffBase)
	}
}

func TestRetryBackoff(t *testing.T) {
	base := 100 * time.Millisecond
	max := time.Second

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 100 * time.Millisecond}, // clamped to attempt 1
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{5, time.Second}, // capped
		{50, time.Second},
	}

	for _, c := range cases {
		if got := RetryBackoff(base, max, c.attempt); got != c.want {
			t.Errorf("RetryBackoff(attempt=%d) = %v, want %v", c.attempt, got, c.want)
		}
	}
}

func TestRetryBackoffOverflowGuard(t *testing.T) {
	// A huge attempt count must cap at max instead of overflowing.
	if got := RetryBackoff(time.Second, time.Hour, 500); got != time.Hour {
		t.Fatalf("RetryBackoff = %v, want %v", got, time.Hour)
	}
}
