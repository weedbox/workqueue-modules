package postgres_workqueue

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

// pgListener holds one dedicated PostgreSQL connection in LISTEN mode and
// forwards notifications to onWake. LISTEN is session-scoped, so this
// connection lives outside GORM's pool. The loop reconnects with backoff on
// any error; notifications emitted while disconnected are lost, which is why
// every (re)connect fires onWake("") — wake everything and let workers
// re-check the table — and why consumers keep a slow poll as a safety net.
type pgListener struct {
	dsn     string
	channel string
	logger  *zap.Logger

	// onWake receives the notification payload (a queue name); "" means
	// "wake all queues".
	onWake func(queue string)

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func newPGListener(dsn, channel string, logger *zap.Logger, onWake func(queue string)) *pgListener {
	ctx, cancel := context.WithCancel(context.Background())

	l := &pgListener{
		dsn:     dsn,
		channel: channel,
		logger:  logger,
		onWake:  onWake,
		ctx:     ctx,
		cancel:  cancel,
		done:    make(chan struct{}),
	}

	go l.run()
	return l
}

func (l *pgListener) run() {
	defer close(l.done)

	const (
		minBackoff = time.Second
		maxBackoff = 30 * time.Second
	)
	backoff := minBackoff

	for l.ctx.Err() == nil {
		if err := l.listenOnce(); err != nil && l.ctx.Err() == nil {
			l.logger.Warn("LISTEN connection lost; reconnecting",
				zap.String("channel", l.channel),
				zap.Duration("backoff", backoff),
				zap.Error(err),
			)

			select {
			case <-l.ctx.Done():
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}
		backoff = minBackoff
	}
}

// listenOnce connects, subscribes, and blocks receiving notifications until
// the connection breaks or the listener is closed.
func (l *pgListener) listenOnce() error {

	conn, err := pgx.Connect(l.ctx, l.dsn)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	if _, err := conn.Exec(l.ctx, "LISTEN "+pgx.Identifier{l.channel}.Sanitize()); err != nil {
		return err
	}

	// Anything NOTIFYed while we were disconnected is gone; wake every queue
	// so workers re-check the table.
	l.onWake("")

	for {
		n, err := conn.WaitForNotification(l.ctx)
		if err != nil {
			return err
		}
		l.onWake(n.Payload)
	}
}

// Close stops the listener and waits for its connection to shut down.
func (l *pgListener) Close() {
	l.cancel()
	<-l.done
}
