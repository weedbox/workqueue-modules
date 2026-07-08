// Package gorm_workqueue provides a database-backed implementation of
// workqueue.WorkQueue on top of any database.DatabaseConnector (PostgreSQL,
// SQLite, ...). Tasks are durable rows; consumers poll for ready tasks and
// claim them with an optimistic status transition, so multiple processes can
// safely compete for the same queue.
package gorm_workqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/weedbox/common-modules/database"
	"github.com/weedbox/weedbox/fxmodule"
	"github.com/weedbox/workqueue-modules/workqueue"
)

// Defaults for Config fields and their viper keys.
const (
	DefaultTableName     = "workqueue_tasks"
	DefaultPollInterval  = time.Second
	DefaultLeaseDuration = 5 * time.Minute
)

// Config controls the storage and polling behaviour of the backend.
type Config struct {
	// TableName is the table tasks are stored in.
	TableName string

	// PollInterval is how often idle workers look for ready tasks. Local
	// enqueues wake workers immediately and idle workers sleep exactly until
	// the next scheduled task comes due, so this mainly bounds pickup latency
	// for tasks enqueued by other processes.
	PollInterval time.Duration

	// LeaseDuration bounds how long one delivery attempt may run before the
	// task is considered abandoned and handed back to the pending state
	// (e.g. after a worker crash). Handlers running longer than this may be
	// delivered a second time.
	LeaseDuration time.Duration
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
	return c
}

// GormWorkQueue is a database-backed workqueue.WorkQueue implementation.
type GormWorkQueue struct {
	db     *gorm.DB
	logger *zap.Logger
	config Config

	mu       sync.Mutex
	notifies map[string]chan struct{}
}

// Compile-time check that GormWorkQueue satisfies the shared interface.
var _ workqueue.WorkQueue = (*GormWorkQueue)(nil)

type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *zap.Logger
	Database  database.DatabaseConnector
}

// Module registers the database backend as an implementation of
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

			q := &GormWorkQueue{
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
					}.withDefaults()
					q.db = p.Database.GetDB()

					q.logger.Info("Starting GormWorkQueue",
						zap.String("table_name", q.config.TableName),
						zap.Duration("poll_interval", q.config.PollInterval),
						zap.Duration("lease_duration", q.config.LeaseDuration),
					)

					if viper.GetBool(getConfigPath("auto_migrate")) {
						return q.AutoMigrate()
					}
					return nil
				},
				OnStop: func(ctx context.Context) error {
					q.logger.Info("Stopped GormWorkQueue")
					return nil
				},
			})

			return q
		},
	)
}

// New creates a GormWorkQueue outside of fx. A nil logger is replaced with a
// no-op logger; zero Config fields fall back to defaults. Callers are
// responsible for running AutoMigrate (or providing the table themselves).
func New(db *gorm.DB, logger *zap.Logger, config Config) *GormWorkQueue {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &GormWorkQueue{
		db:       db,
		logger:   logger,
		config:   config.withDefaults(),
		notifies: make(map[string]chan struct{}),
	}
}

// AutoMigrate creates or updates the task table.
func (q *GormWorkQueue) AutoMigrate() error {
	return q.db.Table(q.config.TableName).AutoMigrate(&TaskRecord{})
}

func (q *GormWorkQueue) table(ctx context.Context) *gorm.DB {
	return q.db.WithContext(ctx).Table(q.config.TableName)
}

// notify returns the current wake channel for a queue; broadcastLocal closes
// and replaces it so idle local workers pick up new tasks without waiting for
// the next poll tick.
func (q *GormWorkQueue) notify(queue string) chan struct{} {
	q.mu.Lock()
	defer q.mu.Unlock()

	ch, ok := q.notifies[queue]
	if !ok {
		ch = make(chan struct{})
		q.notifies[queue] = ch
	}
	return ch
}

func (q *GormWorkQueue) broadcastLocal(queue string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if ch, ok := q.notifies[queue]; ok {
		close(ch)
	}
	q.notifies[queue] = make(chan struct{})
}

// Enqueue adds a task to the named queue.
func (q *GormWorkQueue) Enqueue(ctx context.Context, queue string, payload []byte, opts ...workqueue.EnqueueOption) (*workqueue.Task, error) {

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

	if err := q.table(ctx).Create(record).Error; err != nil {
		return nil, err
	}

	q.broadcastLocal(queue)

	return record.toTask()
}

// reapExpiredLeases hands abandoned running tasks back to the pending state.
func (q *GormWorkQueue) reapExpiredLeases(ctx context.Context, queue string) {

	err := q.table(ctx).
		Where("queue = ? AND status = ? AND lease_expires_at < ?", queue, StatusRunning, time.Now()).
		Updates(map[string]any{
			"status":           StatusPending,
			"lease_expires_at": nil,
		}).Error
	if err != nil {
		q.logger.Warn("Failed to reap expired leases", zap.String("queue", queue), zap.Error(err))
	}
}

// claim attempts to atomically move one ready task to the running state.
// When no task is ready it returns how long to wait until the next scheduled
// task comes due (bounded by the poll interval).
func (q *GormWorkQueue) claim(ctx context.Context, queue string) (*TaskRecord, time.Duration, error) {

	now := time.Now()

	// A small candidate window keeps competing workers from thrashing on a
	// single row.
	var candidates []TaskRecord
	err := q.table(ctx).
		Where("queue = ? AND status = ? AND run_at <= ?", queue, StatusPending, now).
		Order("run_at").Order("enqueued_at").
		Limit(5).
		Find(&candidates).Error
	if err != nil {
		return nil, q.config.PollInterval, err
	}

	for i := range candidates {
		c := &candidates[i]

		// The attempts guard makes the claim fully optimistic: a worker
		// holding a stale candidate (the task failed and was rescheduled in
		// between) loses here instead of double-delivering the same attempt.
		lease := now.Add(q.config.LeaseDuration)
		res := q.table(ctx).
			Where("id = ? AND status = ? AND attempts = ?", c.ID, StatusPending, c.Attempts).
			Updates(map[string]any{
				"status":           StatusRunning,
				"attempts":         gorm.Expr("attempts + 1"),
				"lease_expires_at": lease,
			})
		if res.Error != nil {
			return nil, q.config.PollInterval, res.Error
		}
		if res.RowsAffected == 0 {
			continue // lost the race to another worker
		}

		c.Status = StatusRunning
		c.Attempts++
		c.LeaseExpiresAt = &lease
		return c, 0, nil
	}

	// Nothing ready: sleep until the next scheduled task comes due (delayed
	// tasks, retry backoffs) or the poll interval, whichever is sooner.
	var next TaskRecord
	res := q.table(ctx).
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
			// The task became ready between the two queries (or we lost every
			// candidate above); re-check almost immediately rather than
			// sleeping a negative duration.
			return nil, max(wait, time.Millisecond), nil
		}
	}

	return nil, q.config.PollInterval, nil
}

// complete finalizes one delivery attempt. The attempts guard ensures a
// worker whose lease expired mid-flight cannot clobber a newer claim.
func (q *GormWorkQueue) complete(ctx context.Context, record *TaskRecord, handlerErr error, o *workqueue.ConsumeOptions) {

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

	// Idle local workers recompute their wait so the retry fires on time
	// instead of on the next poll tick.
	q.broadcastLocal(record.Queue)
}

// Consume starts a worker pool polling the named queue.
func (q *GormWorkQueue) Consume(ctx context.Context, queue string, handler workqueue.Handler, opts ...workqueue.ConsumeOption) (workqueue.Subscription, error) {

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

func (q *GormWorkQueue) worker(queue string, handler workqueue.Handler, o *workqueue.ConsumeOptions, stopCh <-chan struct{}) {

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
func (q *GormWorkQueue) ListDeadTasks(ctx context.Context, queue string, limit, offset int) ([]*workqueue.Task, error) {

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
// attempt counter reset.
func (q *GormWorkQueue) RequeueDeadTask(ctx context.Context, queue string, taskID string) error {

	if err := q.ready(); err != nil {
		return err
	}

	res := q.table(ctx).
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

	q.broadcastLocal(queue)
	return nil
}

// DeleteDeadTask permanently removes a dead task.
func (q *GormWorkQueue) DeleteDeadTask(ctx context.Context, queue string, taskID string) error {

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

// subscription implements workqueue.Subscription for the database backend.
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

// errNotStarted is returned when a method runs before fx OnStart resolved
// the database handle (or before New was given one).
var errNotStarted = errors.New("gorm_workqueue: not started yet")

// ready guards public methods against use before the database handle exists.
func (q *GormWorkQueue) ready() error {
	if q.db == nil {
		return errNotStarted
	}
	return nil
}
