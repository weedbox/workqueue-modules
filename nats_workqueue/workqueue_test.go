package nats_workqueue

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	"github.com/weedbox/workqueue-modules/workqueue"
	"github.com/weedbox/workqueue-modules/workqueuetest"
)

// newTestQueue spins up an embedded JetStream-enabled NATS server and returns
// a NATSWorkQueue connected to it. Everything is torn down via t.Cleanup.
func newTestQueue(t *testing.T) *NATSWorkQueue {
	t.Helper()

	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()

	srv := natsserver.RunServer(&opts)
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatal("nats test server not ready")
	}
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	t.Cleanup(nc.Close)

	q, err := New(nc, zap.NewNop(), Config{
		// Short AckWait keeps redelivery of shutdown-nak'd messages fast.
		AckWait: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return q
}

func TestConformance(t *testing.T) {
	workqueuetest.Run(t, func(t *testing.T) workqueue.WorkQueue {
		return newTestQueue(t)
	})
}

func TestInvalidQueueName(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	for _, name := range []string{"", "a.b", "a b", "a*", "a>"} {
		if _, err := q.Enqueue(ctx, name, nil); err == nil {
			t.Errorf("Enqueue(%q) succeeded, want ErrInvalidQueueName", name)
		}
	}
}

func TestNotStartedReturnsError(t *testing.T) {
	q := &NATSWorkQueue{logger: zap.NewNop()}

	if _, err := q.Enqueue(context.Background(), "x", nil); err != errNotStarted {
		t.Fatalf("Enqueue = %v, want errNotStarted", err)
	}
	if _, err := q.Consume(context.Background(), "x", nil); err != errNotStarted {
		t.Fatalf("Consume = %v, want errNotStarted", err)
	}
}
