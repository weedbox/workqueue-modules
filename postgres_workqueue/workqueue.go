// Package postgres_workqueue provides a PostgreSQL-native implementation of
// workqueue.WorkQueue. It shares gorm_workqueue's table layout but claims
// tasks with a single FOR UPDATE SKIP LOCKED round-trip (contention-free
// under many competing workers) and wakes remote workers through
// LISTEN/NOTIFY, so the poll interval degrades into a slow safety net
// instead of driving delivery latency.
package postgres_workqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
	gormpostgres "gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/weedbox/common-modules/database"
	"github.com/weedbox/weedbox/fxmodule"
	"github.com/weedbox/workqueue-modules/workqueue"
)

// Defaults for Config fields and their viper keys.
const (
	DefaultTableName     = "workqueue_tasks"
	DefaultPollInterval  = 30 * time.Second
	DefaultLeaseDuration = 5 * time.Minute
)

// errNotStarted is returned when a method runs before fx OnStart resolved
// the database handle (or before New was given one).
var errNotStarted = errors.New("postgres_workqueue: not started yet")

// Config controls the storage, notification, and polling behaviour.
type Config struct {
	// TableName is the table tasks are stored in. The layout is identical to
	// gorm_workqueue's, so both backends can share a table.
	TableName string

	// PollInterval is the safety-net poll for idle workers. LISTEN/NOTIFY
	// delivers wakeups immediately, so this only bounds pickup latency when
	// notifications are lost (listener reconnecting) or disabled.
	PollInterval time.Duration

	// LeaseDuration bounds how long one delivery attempt may run before the
	// task is considered abandoned and handed back to the pending state
	// (e.g. after a worker crash). Handlers running longer than this may be
	// delivered a second time.
	LeaseDuration time.Duration

	// NotifyChannel is the LISTEN/NOTIFY channel used for cross-process
	// wakeups. Defaults to "<table_name>_wakeup".
	NotifyChannel string

	// ListenDSN is the connection string for the dedicated LISTEN
	// connection. When empty it is extracted from the *gorm.DB's postgres
	// dialector; if that fails too, the backend runs in poll-only mode.
	ListenDSN string
}

func (c Config) withDefaults() Config {
	if c.TableName == "" {
		c.TableName = DefaultTableName
	}
	if c.PollInterval <= 0 {
		c.PollInterval = DefaultPollInterval
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = DefaultLeaseDuration
	}
	if c.NotifyChannel == "" {
		c.NotifyChannel = c.TableName + "_wakeup"
	}
	return c
}

// PostgresWorkQueue is a PostgreSQL-backed workqueue.WorkQueue
// implementation.
type PostgresWorkQueue struct {
	db     *gorm.DB
	logger *zap.Logger
	config Config

	mu       sync.Mutex
	notifies map[string]chan struct{}

	listener *pgListener
}

// Compile-time check that PostgresWorkQueue satisfies the shared interface.
var _ workqueue.WorkQueue = (*PostgresWorkQueue)(nil)

type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *zap.Logger
	Database  database.DatabaseConnector
}

// Module registers the PostgreSQL backend as an implementation of
// workqueue.WorkQueue. It can be loaded on its own (inject the interface
// without a name tag) or side by side with other backends (inject with
// name:"<scope>").
func Module(scope string) fx.Option {
	return fxmodule.InterfaceModule[workqueue.WorkQueue](
		scope,
		func(p Params) workqueue.WorkQueue {

			getConfigPath := func(key string) string {
				return fmt.Sprintf("%s.%s", scope, key)
			}

			viper.SetDefault(getConfigPath("table_name"), DefaultTableName)
			viper.SetDefault(getConfigPath("poll_interval"), DefaultPollInterval.String())
			viper.SetDefault(getConfigPath("lease_duration"), DefaultLeaseDuration.String())
			viper.SetDefault(getConfigPath("auto_migrate"), true)
			viper.SetDefault(getConfigPath("notify_channel"), "")
			viper.SetDefault(getConfigPath("listen_dsn"), "")

			q := &PostgresWorkQueue{
				logger:   p.Logger.Named(scope),
				notifies: make(map[string]chan struct{}),
			}

			p.Lifecycle.Append(fx.Hook{
				// The database connector only opens its connection in its own
				// OnStart, so the handle and config are resolved here rather
				// than at construction time.
				OnStart: func(ctx context.Context) error {
					q.config = Config{
						TableName:     viper.GetString(getConfigPath("table_name")),
						PollInterval:  viper.GetDuration(getConfigPath("poll_interval")),
						LeaseDuration: viper.GetDuration(getConfigPath("lease_duration")),
						NotifyChannel: viper.GetString(getConfigPath("notify_channel")),
						ListenDSN:     viper.GetString(getConfigPath("listen_dsn")),
					}.withDefaults()
					q.db = p.Database.GetDB()

					q.logger.Info("Starting PostgresWorkQueue",
						zap.String("table_name", q.config.TableName),
						zap.Duration("poll_interval", q.config.PollInterval),
						zap.Duration("lease_duration", q.config.LeaseDuration),
						zap.String("notify_channel", q.config.NotifyChannel),
					)

					if viper.GetBool(getConfigPath("auto_migrate")) {
						if err := q.AutoMigrate(); err != nil {
							return err
						}
					}

					q.startListener()
					return nil
				},
				OnStop: func(ctx context.Context) error {
					q.Close()
					q.logger.Info("Stopped PostgresWorkQueue")
					return nil
				},
			})

			return q
		},
	)
}

// New creates a PostgresWorkQueue outside of fx and starts its LISTEN
// connection. A nil logger is replaced with a no-op logger; zero Config
// fields fall back to defaults. Callers are responsible for running
// AutoMigrate (or providing the table themselves) and for calling Close when
// done.
func New(db *gorm.DB, logger *zap.Logger, config Config) *PostgresWorkQueue {
	if logger == nil {
		logger = zap.NewNop()
	}

	q := &PostgresWorkQueue{
		db:       db,
		logger:   logger,
		config:   config.withDefaults(),
		notifies: make(map[string]chan struct{}),
	}
	q.startListener()

	return q
}

// startListener spins up the dedicated LISTEN connection. Without a usable
// DSN the backend still works, falling back to pure polling.
func (q *PostgresWorkQueue) startListener() {

	dsn := q.config.ListenDSN
	if dsn == "" {
		if d, ok := q.db.Dialector.(*gormpostgres.Dialector); ok && d.Config != nil {
			dsn = d.Config.DSN
		}
	}
	if dsn == "" {
		q.logger.Warn("No DSN available for LISTEN/NOTIFY; running in poll-only mode " +
			"(cross-process pickup latency is bounded by poll_interval)")
		return
	}

	q.listener = newPGListener(dsn, q.config.NotifyChannel, q.logger, func(queue string) {
		if queue == "" {
			q.broadcastAll()
		} else {
			q.broadcastLocal(queue)
		}
	})
}

// Close stops the LISTEN connection. Existing subscriptions keep working in
// poll-only mode; new wakeups arrive via the poll interval.
func (q *PostgresWorkQueue) Close() {
	if q.listener != nil {
		q.listener.Close()
		q.listener = nil
	}
}

// AutoMigrate creates or updates the task table.
func (q *PostgresWorkQueue) AutoMigrate() error {
	return q.db.Table(q.config.TableName).AutoMigrate(&TaskRecord{})
}

// ready guards public methods against use before the database handle exists.
func (q *PostgresWorkQueue) ready() error {
	if q.db == nil {
		return errNotStarted
	}
	return nil
}

func (q *PostgresWorkQueue) table(ctx context.Context) *gorm.DB {
	return q.db.WithContext(ctx).Table(q.config.TableName)
}

// quotedTable returns the configured table name as a quoted identifier for
// use in raw SQL.
func (q *PostgresWorkQueue) quotedTable() string {
	return `"` + strings.ReplaceAll(q.config.TableName, `"`, `""`) + `"`
}

// notify returns the current wake channel for a queue; broadcastLocal closes
// and replaces it. Workers capture the channel BEFORE querying the table so
// a notification arriving mid-query still wakes them.
func (q *PostgresWorkQueue) notify(queue string) chan struct{} {
	q.mu.Lock()
	defer q.mu.Unlock()

	ch, ok := q.notifies[queue]
	if !ok {
		ch = make(chan struct{})
		q.notifies[queue] = ch
	}
	return ch
}

func (q *PostgresWorkQueue) broadcastLocal(queue string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if ch, ok := q.notifies[queue]; ok {
		close(ch)
	}
	q.notifies[queue] = make(chan struct{})
}

func (q *PostgresWorkQueue) broadcastAll() {
	q.mu.Lock()
	defer q.mu.Unlock()

	for queue, ch := range q.notifies {
		close(ch)
		q.notifies[queue] = make(chan struct{})
	}
}

// Enqueue adds a task to the named queue. The NOTIFY rides in the same
// transaction as the INSERT, so listeners are only woken once the row is
// visible.
func (q *PostgresWorkQueue) Enqueue(ctx context.Context, queue string, payload []byte, opts ...workqueue.EnqueueOption) (*workqueue.Task, error) {

	if err := q.ready(); err != nil {
		return nil, err
	}

	o := workqueue.NewEnqueueOptions(opts...)
	now := time.Now()

	var metadata []byte
	if len(o.Metadata) > 0 {
		var err error
		metadata, err = json.Marshal(o.Metadata)
		if err != nil {
			return nil, err
		}
	}

	record := &TaskRecord{
		ID:         uuid.NewString(),
		Queue:      queue,
		Status:     StatusPending,
		RunAt:      o.ResolveRunAt(now),
		Payload:    payload,
		Metadata:   metadata,
		MaxRetries: o.MaxRetries,
		EnqueuedAt: now,
	}

	err := q.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Table(q.config.TableName).Create(record).Error; err != nil {
			return err
		}
		return tx.Exec("SELECT pg_notify(?, ?)", q.config.NotifyChannel, queue).Error
	})
	if err != nil {
		return nil, err
	}

	q.broadcastLocal(queue)

	return record.toTask()
}

// claim atomically moves one ready task to the running state using
// FOR UPDATE SKIP LOCKED — a single round-trip with no retry loop, however
// many workers compete. When nothing is ready it returns how long to wait
// until the next scheduled task (bounded by the poll interval).
func (q *PostgresWorkQueue) claim(ctx context.Context, queue string) (*TaskRecord, time.Duration, error) {

	now := time.Now()
	lease := now.Add(q.config.LeaseDuration)
	tbl := q.quotedTable()

	claimSQL := fmt.Sprintf(`
		UPDATE %s SET
			status = ?,
			attempts = attempts + 1,
			lease_expires_at = ?
		WHERE id = (
			SELECT id FROM %s
			WHERE queue = ? AND status = ? AND run_at <= ?
			ORDER BY run_at, enqueued_at
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING *`, tbl, tbl)

	var record TaskRecord
	res := q.db.WithContext(ctx).Raw(claimSQL,
		StatusRunning, lease,
		queue, StatusPending, now,
	).Scan(&record)
	if res.Error != nil {
		return nil, q.config.PollInterval, res.Error
	}
	if res.RowsAffected > 0 {
		return &record, 0, nil
	}

	// Nothing ready: sleep until the next scheduled task comes due (delayed
	// tasks, retry backoffs) or the poll interval, whichever is sooner.
	var next TaskRecord
	res = q.table(ctx).
		Select("run_at").
		Where("queue = ? AND status = ?", queue, StatusPending).
		Order("run_at").
		Limit(1).
		Scan(&next)
	if res.Error != nil {
		return nil, q.config.PollInterval, res.Error
	}
	if res.RowsAffected > 0 {
		if wait := time.Until(next.RunAt); wait < q.config.PollInterval {
			// A pending task appeared between the two queries; re-check
			// almost immediately rather than sleeping a negative duration.
			return nil, max(wait, time.Millisecond), nil
		}
	}

	return nil, q.config.PollInterval, nil
}

// reapExpiredLeases hands abandoned running tasks back to the pending state
// and notifies other processes when it recovered any.
func (q *PostgresWorkQueue) reapExpiredLeases(ctx context.Context, queue string) {

	res := q.table(ctx).
		Where("queue = ? AND status = ? AND lease_expires_at < ?", queue, StatusRunning, time.Now()).
		Updates(map[string]any{
			"status":           StatusPending,
			"lease_expires_at": nil,
		})
	if res.Error != nil {
		q.logger.Warn("Failed to reap expired leases", zap.String("queue", queue), zap.Error(res.Error))
		return
	}

	if res.RowsAffected > 0 {
		if err := q.db.WithContext(ctx).
			Exec("SELECT pg_notify(?, ?)", q.config.NotifyChannel, queue).Error; err != nil {
			q.logger.Warn("Failed to notify after lease reap", zap.String("queue", queue), zap.Error(err))
		}
	}
}

// complete finalizes one delivery attempt. The attempts guard ensures a
// worker whose lease expired mid-flight cannot clobber a newer claim.
func (q *PostgresWorkQueue) complete(ctx context.Context, record *TaskRecord, handlerErr error, o *workqueue.ConsumeOptions) {

	if handlerErr == nil {
		err := q.table(ctx).
			Where("id = ?", record.ID).
			Delete(&TaskRecord{}).Error
		if err != nil {
			q.logger.Error("Failed to delete completed task",
				zap.String("task_id", record.ID), zap.Error(err))
		}
		return
	}

	guard := q.table(ctx).
		Where("id = ? AND status = ? AND attempts = ?", record.ID, StatusRunning, record.Attempts)

	if record.Attempts > record.MaxRetries {
		err := guard.Updates(map[string]any{
			"status":           StatusDead,
			"last_error":       handlerErr.Error(),
			"died_at":          time.Now(),
			"lease_expires_at": nil,
		}).Error
		if err != nil {
			q.logger.Error("Failed to move task to dead-letter state",
				zap.String("task_id", record.ID), zap.Error(err))
			return
		}

		q.logger.Warn("Task moved to dead-letter queue",
			zap.String("queue", record.Queue),
			zap.String("task_id", record.ID),
			zap.Int("attempts", record.Attempts),
			zap.Error(handlerErr),
		)
		return
	}

	runAt := time.Now().Add(workqueue.RetryBackoff(o.BackoffBase, o.BackoffMax, record.Attempts))
	err := guard.Updates(map[string]any{
		"status":           StatusPending,
		"run_at":           runAt,
		"last_error":       handlerErr.Error(),
		"lease_expires_at": nil,
	}).Error
	if err != nil {
		q.logger.Error("Failed to reschedule failed task",
			zap.String("task_id", record.ID), zap.Error(err))
		return
	}

	// Local workers recompute their wait; the retry stays in-process, so no
	// cross-process NOTIFY is needed (the poll safety net covers a crash).
	q.broadcastLocal(record.Queue)
}

// Consume starts a worker pool delivering tasks from the named queue.
func (q *PostgresWorkQueue) Consume(ctx context.Context, queue string, handler workqueue.Handler, opts ...workqueue.ConsumeOption) (workqueue.Subscription, error) {

	if err := q.ready(); err != nil {
		return nil, err
	}

	o := workqueue.NewConsumeOptions(opts...)

	sub := newSubscription()

	var wg sync.WaitGroup
	for range o.Concurrency {
		wg.Go(func() {
			q.worker(queue, handler, o, sub.stopCh)
		})
	}

	go func() {
		wg.Wait()
		close(sub.done)
	}()

	return sub, nil
}

func (q *PostgresWorkQueue) worker(queue string, handler workqueue.Handler, o *workqueue.ConsumeOptions, stopCh <-chan struct{}) {

	ctx := context.Background()

	for {
		select {
		case <-stopCh:
			return
		default:
		}

		// Capture the wake channel before querying: an enqueue landing
		// between the claim query and the sleep still closes this channel.
		notify := q.notify(queue)

		record, wait, err := q.claim(ctx, queue)
		if err != nil {
			q.logger.Warn("Failed to claim task", zap.String("queue", queue), zap.Error(err))
		}

		if record != nil {
			task, convErr := record.toTask()
			if convErr != nil {
				q.complete(ctx, record, fmt.Errorf("decode task: %w", convErr), o)
				continue
			}
			q.complete(ctx, record, invokeHandler(handler, task), o)
			continue
		}

		q.reapExpiredLeases(ctx, queue)

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
func (q *PostgresWorkQueue) ListDeadTasks(ctx context.Context, queue string, limit, offset int) ([]*workqueue.Task, error) {

	if err := q.ready(); err != nil {
		return nil, err
	}

	if offset < 0 {
		offset = 0
	}

	query := q.table(ctx).
		Where("queue = ? AND status = ?", queue, StatusDead).
		Order("died_at").Order("id").
		Offset(offset)
	if limit > 0 {
		query = query.Limit(limit)
	}

	var records []TaskRecord
	if err := query.Find(&records).Error; err != nil {
		return nil, err
	}

	out := make([]*workqueue.Task, 0, len(records))
	for i := range records {
		task, err := records[i].toTask()
		if err != nil {
			return nil, err
		}
		out = append(out, task)
	}
	return out, nil
}

// RequeueDeadTask moves a dead task back to the pending queue with its
// attempt counter reset, waking local and remote workers.
func (q *PostgresWorkQueue) RequeueDeadTask(ctx context.Context, queue string, taskID string) error {

	if err := q.ready(); err != nil {
		return err
	}

	err := q.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Table(q.config.TableName).
			Where("id = ? AND queue = ? AND status = ?", taskID, queue, StatusDead).
			Updates(map[string]any{
				"status":     StatusPending,
				"attempts":   0,
				"last_error": "",
				"died_at":    nil,
				"run_at":     time.Now(),
			})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return workqueue.ErrTaskNotFound
		}
		return tx.Exec("SELECT pg_notify(?, ?)", q.config.NotifyChannel, queue).Error
	})
	if err != nil {
		return err
	}

	q.broadcastLocal(queue)
	return nil
}

// DeleteDeadTask permanently removes a dead task.
func (q *PostgresWorkQueue) DeleteDeadTask(ctx context.Context, queue string, taskID string) error {

	if err := q.ready(); err != nil {
		return err
	}

	res := q.table(ctx).
		Where("id = ? AND queue = ? AND status = ?", taskID, queue, StatusDead).
		Delete(&TaskRecord{})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return workqueue.ErrTaskNotFound
	}
	return nil
}

// subscription implements workqueue.Subscription for the PostgreSQL backend.
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
