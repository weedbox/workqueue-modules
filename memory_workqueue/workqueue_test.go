package memory_workqueue

import (
	"context"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/weedbox/weedbox/fxmodule"
	"github.com/weedbox/workqueue-modules/workqueue"
	"github.com/weedbox/workqueue-modules/workqueuetest"
)

func TestConformance(t *testing.T) {
	workqueuetest.Run(t, func(t *testing.T) workqueue.WorkQueue {
		return New(zap.NewNop())
	})
}

func TestModuleWiring(t *testing.T) {
	fxmodule.ResetClaim[workqueue.WorkQueue]()
	t.Cleanup(func() { fxmodule.ResetClaim[workqueue.WorkQueue]() })

	var q workqueue.WorkQueue

	app := fx.New(
		fx.NopLogger,
		fx.Provide(zap.NewNop),
		Module("workqueue"),
		fx.Invoke(func(p struct {
			fx.In
			Default workqueue.WorkQueue
			Named   workqueue.WorkQueue `name:"workqueue"`
		}) {
			if p.Default != p.Named {
				t.Error("unnamed default and named instance differ")
			}
			q = p.Default
		}),
	)

	startCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := app.Start(startCtx); err != nil {
		t.Fatalf("fx start: %v", err)
	}
	defer app.Stop(context.Background())

	if q == nil {
		t.Fatal("WorkQueue not injected")
	}

	// Smoke test through the injected interface.
	if _, err := q.Enqueue(context.Background(), "fx-smoke", []byte("ping")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
}
