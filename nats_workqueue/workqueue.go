// Package nats_workqueue provides a NATS JetStream-backed implementation of
// workqueue.WorkQueue. Each queue maps to a work-queue-retention stream plus
// a browsable dead-letter stream, so tasks are durable and multiple processes
// can compete for the same queue.
//
// Retry bookkeeping lives in the task payload rather than in JetStream
// delivery counts: a failed attempt republishes the task with an incremented
// attempt counter (and a backoff RunAt) and acknowledges the original
// message. Delayed tasks are scheduled by NakWithDelay until RunAt is due,
// which never distorts the attempt count.
package nats_workqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/weedbox/common-modules/nats_connector"
	"github.com/weedbox/weedbox/fxmodule"
	"github.com/weedbox/workqueue-modules/workqueue"
)

// Defaults for Config fields and their viper keys.
const (
	DefaultStreamPrefix  = "WORKQUEUE"
	DefaultSubjectPrefix = "workqueue"
	DefaultAckWait       = 30 * time.Second
)

// ErrInvalidQueueName is returned when a queue name cannot be used as a NATS
// subject token / stream name fragment.
var ErrInvalidQueueName = errors.New("nats_workqueue: invalid queue name")

// errNotStarted is returned when a method runs before fx OnStart resolved
// the NATS connection.
var errNotStarted = errors.New("nats_workqueue: not started yet")

// Config controls stream naming and consumer behaviour.
type Config struct {
	// StreamPrefix prefixes every stream name: "<prefix>_<queue>" for tasks
	// and "<prefix>_<queue>_DLQ" for the dead-letter stream.
	StreamPrefix string

	// SubjectPrefix prefixes every subject: "<prefix>.<queue>.tasks" and
	// "<prefix>.<queue>.dead".
	SubjectPrefix string

	// Replicas is the stream replica count. 0 targets 3 replicas with an
	// automatic fallback to 1 on single-node servers (see the
	// nats_connector Ensure* helpers).
	Replicas int

	// AckWait is the per-delivery acknowledgement deadline. Handlers running
	// longer are kept alive with in-progress heartbeats every AckWait/3.
	AckWait time.Duration
}

func (c Config) withDefaults() Config {
	if c.StreamPrefix == "" {
		c.StreamPrefix = DefaultStreamPrefix
	}
	if c.SubjectPrefix == "" {
		c.SubjectPrefix = DefaultSubjectPrefix
	}
	if c.AckWait <= 0 {
		c.AckWait = DefaultAckWait
	}
	return c
}

// NATSWorkQueue is a JetStream-backed workqueue.WorkQueue implementation.
type NATSWorkQueue struct {
	logger *zap.Logger
	config Config

	js jetstream.JetStream

	mu      sync.Mutex
	ensured map[string]jetstream.Stream // queue -> tasks stream
}

// Compile-time check that NATSWorkQueue satisfies the shared interface.
var _ workqueue.WorkQueue = (*NATSWorkQueue)(nil)

type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *zap.Logger
	NATS      *nats_connector.NATSConnector
}

// Module registers the JetStream backend as an implementation of
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

			viper.SetDefault(getConfigPath("stream_prefix"), DefaultStreamPrefix)
			viper.SetDefault(getConfigPath("subject_prefix"), DefaultSubjectPrefix)
			viper.SetDefault(getConfigPath("replicas"), 0)
			viper.SetDefault(getConfigPath("ack_wait"), DefaultAckWait.String())

			q := &NATSWorkQueue{
				logger:  p.Logger.Named(scope),
				ensured: make(map[string]jetstream.Stream),
			}

			p.Lifecycle.Append(fx.Hook{
				// The NATS connector only connects in its own OnStart, so the
				// JetStream handle is resolved here rather than at
				// construction time.
				OnStart: func(ctx context.Context) error {
					q.config = Config{
						StreamPrefix:  viper.GetString(getConfigPath("stream_prefix")),
						SubjectPrefix: viper.GetString(getConfigPath("subject_prefix")),
						Replicas:      viper.GetInt(getConfigPath("replicas")),
						AckWait:       viper.GetDuration(getConfigPath("ack_wait")),
					}.withDefaults()
					q.js = p.NATS.GetJetStream()

					q.logger.Info("Starting NATSWorkQueue",
						zap.String("stream_prefix", q.config.StreamPrefix),
						zap.String("subject_prefix", q.config.SubjectPrefix),
						zap.Int("replicas", q.config.Replicas),
						zap.Duration("ack_wait", q.config.AckWait),
					)
					return nil
				},
				OnStop: func(ctx context.Context) error {
					q.logger.Info("Stopped NATSWorkQueue")
					return nil
				},
			})

			return q
		},
	)
}

// New creates a NATSWorkQueue outside of fx on an existing NATS connection.
// A nil logger is replaced with a no-op logger; zero Config fields fall back
// to defaults.
func New(conn *nats.Conn, logger *zap.Logger, config Config) (*NATSWorkQueue, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	js, err := jetstream.New(conn)
	if err != nil {
		return nil, err
	}

	return &NATSWorkQueue{
		logger:  logger,
		config:  config.withDefaults(),
		js:      js,
		ensured: make(map[string]jetstream.Stream),
	}, nil
}

// ready guards public methods against use before the JetStream handle exists.
func (q *NATSWorkQueue) ready() error {
	if q.js == nil {
		return errNotStarted
	}
	return nil
}

// validateQueue rejects names that would break subjects or stream names.
func validateQueue(queue string) error {
	if queue == "" || strings.ContainsAny(queue, ".*> /\\\t\n") {
		return fmt.Errorf("%w: %q", ErrInvalidQueueName, queue)
	}
	return nil
}

func (q *NATSWorkQueue) streamName(queue string) string {
	return fmt.Sprintf("%s_%s", q.config.StreamPrefix, queue)
}

func (q *NATSWorkQueue) dlqStreamName(queue string) string {
	return fmt.Sprintf("%s_%s_DLQ", q.config.StreamPrefix, queue)
}

func (q *NATSWorkQueue) tasksSubject(queue string) string {
	return fmt.Sprintf("%s.%s.tasks", q.config.SubjectPrefix, queue)
}

func (q *NATSWorkQueue) deadSubject(queue string) string {
	return fmt.Sprintf("%s.%s.dead", q.config.SubjectPrefix, queue)
}

func (q *NATSWorkQueue) consumerName(queue string) string {
	return fmt.Sprintf("%s_%s_workers", q.config.StreamPrefix, queue)
}

// ensureQueue provisions the tasks stream and dead-letter stream for a queue
// (multi-instance safe) and caches the result.
func (q *NATSWorkQueue) ensureQueue(ctx context.Context, queue string) (jetstream.Stream, error) {

	if err := validateQueue(queue); err != nil {
		return nil, err
	}

	q.mu.Lock()
	stream, ok := q.ensured[queue]
	q.mu.Unlock()
	if ok {
		return stream, nil
	}

	ensureOpts := []nats_connector.EnsureOption{
		nats_connector.WithEnsureLogger(q.logger),
		// Fall back to a single replica immediately on single-node servers.
		nats_connector.WithInsufficientPeersBudget(0),
	}

	stream, err := nats_connector.EnsureStream(ctx, q.js, jetstream.StreamConfig{
		Name:      q.streamName(queue),
		Subjects:  []string{q.tasksSubject(queue)},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.WorkQueuePolicy,
		Replicas:  q.config.Replicas,
	}, ensureOpts...)
	if err != nil {
		return nil, fmt.Errorf("ensure tasks stream: %w", err)
	}

	_, err = nats_connector.EnsureStream(ctx, q.js, jetstream.StreamConfig{
		Name:      q.dlqStreamName(queue),
		Subjects:  []string{q.deadSubject(queue)},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		Replicas:  q.config.Replicas,
	}, ensureOpts...)
	if err != nil {
		return nil, fmt.Errorf("ensure dead-letter stream: %w", err)
	}

	q.mu.Lock()
	q.ensured[queue] = stream
	q.mu.Unlock()

	return stream, nil
}

// publishTask publishes a task JSON message. A non-empty msgID enables
// JetStream dedup for the crash-between-publish-and-ack window.
func (q *NATSWorkQueue) publishTask(ctx context.Context, subject string, task *workqueue.Task, msgID string) error {

	data, err := json.Marshal(task)
	if err != nil {
		return err
	}

	var opts []jetstream.PublishOpt
	if msgID != "" {
		opts = append(opts, jetstream.WithMsgID(msgID))
	}

	_, err = q.js.Publish(ctx, subject, data, opts...)
	return err
}

// attemptMsgID builds the dedup ID for a given delivery attempt of a task.
func attemptMsgID(taskID string, attempts int) string {
	return fmt.Sprintf("wq:%s:%d", taskID, attempts)
}

// Enqueue adds a task to the named queue.
func (q *NATSWorkQueue) Enqueue(ctx context.Context, queue string, payload []byte, opts ...workqueue.EnqueueOption) (*workqueue.Task, error) {

	if err := q.ready(); err != nil {
		return nil, err
	}
	if _, err := q.ensureQueue(ctx, queue); err != nil {
		return nil, err
	}

	o := workqueue.NewEnqueueOptions(opts...)
	now := time.Now()

	task := &workqueue.Task{
		ID:         uuid.NewString(),
		Queue:      queue,
		Payload:    payload,
		Metadata:   o.Metadata,
		MaxRetries: o.MaxRetries,
		EnqueuedAt: now,
		RunAt:      o.ResolveRunAt(now),
	}

	if err := q.publishTask(ctx, q.tasksSubject(queue), task, attemptMsgID(task.ID, 0)); err != nil {
		return nil, err
	}

	return task, nil
}

// Consume starts delivering tasks from the named queue.
func (q *NATSWorkQueue) Consume(ctx context.Context, queue string, handler workqueue.Handler, opts ...workqueue.ConsumeOption) (workqueue.Subscription, error) {

	if err := q.ready(); err != nil {
		return nil, err
	}

	stream, err := q.ensureQueue(ctx, queue)
	if err != nil {
		return nil, err
	}

	o := workqueue.NewConsumeOptions(opts...)

	consumer, err := nats_connector.EnsureConsumer(ctx, stream, jetstream.ConsumerConfig{
		Durable:       q.consumerName(queue),
		FilterSubject: q.tasksSubject(queue),
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       q.config.AckWait,
		// Retries are managed by republishing with an incremented attempt
		// counter, so JetStream-level redelivery is unbounded.
		MaxDeliver: -1,
		// Bound unacknowledged deliveries to the worker pool size so
		// prefetched messages don't burn their AckWait in a local buffer.
		MaxAckPending: o.Concurrency,
	}, nats_connector.WithEnsureLogger(q.logger), nats_connector.WithInsufficientPeersBudget(0))
	if err != nil {
		return nil, fmt.Errorf("ensure consumer: %w", err)
	}

	sub := &subscription{
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
		sem:    make(chan struct{}, o.Concurrency),
	}

	consCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		select {
		case sub.sem <- struct{}{}:
		case <-sub.stopCh:
			// Shutting down: hand the message back for redelivery.
			if err := msg.Nak(); err != nil {
				q.logger.Warn("Failed to nak message during shutdown", zap.Error(err))
			}
			return
		}

		sub.wg.Add(1)
		go func() {
			defer func() {
				<-sub.sem
				sub.wg.Done()
			}()
			q.handleMsg(msg, queue, handler, o)
		}()
	})
	if err != nil {
		return nil, err
	}

	sub.consCtx = consCtx
	return sub, nil
}

// handleMsg processes one JetStream delivery.
func (q *NATSWorkQueue) handleMsg(msg jetstream.Msg, queue string, handler workqueue.Handler, o *workqueue.ConsumeOptions) {

	ctx := context.Background()

	var task workqueue.Task
	if err := json.Unmarshal(msg.Data(), &task); err != nil {
		q.logger.Error("Dropping undecodable task message",
			zap.String("queue", queue), zap.Error(err))
		if termErr := msg.TermWithReason("undecodable task payload"); termErr != nil {
			q.logger.Warn("Failed to terminate message", zap.Error(termErr))
		}
		return
	}

	// Not due yet: schedule redelivery at RunAt without consuming an attempt.
	if wait := time.Until(task.RunAt); wait > 0 {
		if err := msg.NakWithDelay(wait); err != nil {
			q.logger.Warn("Failed to delay task", zap.String("task_id", task.ID), zap.Error(err))
		}
		return
	}

	task.Attempts++

	handlerErr := q.invokeWithHeartbeat(msg, handler, &task)

	if handlerErr == nil {
		if err := msg.Ack(); err != nil {
			q.logger.Warn("Failed to ack task", zap.String("task_id", task.ID), zap.Error(err))
		}
		return
	}

	if task.Attempts > task.MaxRetries {
		task.LastError = handlerErr.Error()
		task.DiedAt = time.Now()

		if err := q.publishTask(ctx, q.deadSubject(queue), &task, ""); err != nil {
			q.logger.Error("Failed to publish task to dead-letter stream; leaving for redelivery",
				zap.String("task_id", task.ID), zap.Error(err))
			return // not acked: JetStream redelivers, we try again
		}

		q.logger.Warn("Task moved to dead-letter queue",
			zap.String("queue", queue),
			zap.String("task_id", task.ID),
			zap.Int("attempts", task.Attempts),
			zap.Error(handlerErr),
		)
	} else {
		task.LastError = handlerErr.Error()
		task.RunAt = time.Now().Add(workqueue.RetryBackoff(o.BackoffBase, o.BackoffMax, task.Attempts))

		if err := q.publishTask(ctx, q.tasksSubject(queue), &task, attemptMsgID(task.ID, task.Attempts)); err != nil {
			q.logger.Error("Failed to republish task for retry; leaving for redelivery",
				zap.String("task_id", task.ID), zap.Error(err))
			return // not acked: JetStream redelivers, we try again
		}
	}

	if err := msg.Ack(); err != nil {
		q.logger.Warn("Failed to ack task after handoff", zap.String("task_id", task.ID), zap.Error(err))
	}
}

// invokeWithHeartbeat runs the handler with panic recovery while keeping the
// delivery alive via in-progress heartbeats.
func (q *NATSWorkQueue) invokeWithHeartbeat(msg jetstream.Msg, handler workqueue.Handler, task *workqueue.Task) error {

	interval := max(q.config.AckWait/3, time.Second)

	heartbeatDone := make(chan struct{})
	var heartbeatWG sync.WaitGroup
	heartbeatWG.Go(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatDone:
				return
			case <-ticker.C:
				if err := msg.InProgress(); err != nil {
					q.logger.Warn("Failed to send in-progress heartbeat",
						zap.String("task_id", task.ID), zap.Error(err))
				}
			}
		}
	})

	err := invokeHandler(handler, task)

	close(heartbeatDone)
	heartbeatWG.Wait()

	return err
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

// deadEntry pairs a dead task with its stream sequence for delete/requeue.
type deadEntry struct {
	task *workqueue.Task
	seq  uint64
}

// scanDeadTasks walks the dead-letter stream oldest-first, invoking fn for
// every decodable entry until fn returns false or the stream is exhausted.
func (q *NATSWorkQueue) scanDeadTasks(ctx context.Context, queue string, fn func(deadEntry) bool) error {

	if _, err := q.ensureQueue(ctx, queue); err != nil {
		return err
	}

	stream, err := q.js.Stream(ctx, q.dlqStreamName(queue))
	if err != nil {
		return err
	}

	info, err := stream.Info(ctx)
	if err != nil {
		return err
	}

	for seq := info.State.FirstSeq; seq <= info.State.LastSeq && seq > 0; seq++ {
		raw, err := stream.GetMsg(ctx, seq)
		if err != nil {
			if errors.Is(err, jetstream.ErrMsgNotFound) {
				continue // deleted in the meantime
			}
			return err
		}

		var task workqueue.Task
		if err := json.Unmarshal(raw.Data, &task); err != nil {
			q.logger.Warn("Skipping undecodable dead-letter entry",
				zap.String("queue", queue), zap.Uint64("seq", seq), zap.Error(err))
			continue
		}

		if !fn(deadEntry{task: &task, seq: seq}) {
			return nil
		}
	}

	return nil
}

// ListDeadTasks returns dead tasks ordered oldest first.
func (q *NATSWorkQueue) ListDeadTasks(ctx context.Context, queue string, limit, offset int) ([]*workqueue.Task, error) {

	if err := q.ready(); err != nil {
		return nil, err
	}
	if offset < 0 {
		offset = 0
	}

	var out []*workqueue.Task
	skipped := 0

	err := q.scanDeadTasks(ctx, queue, func(e deadEntry) bool {
		if skipped < offset {
			skipped++
			return true
		}
		out = append(out, e.task)
		return limit <= 0 || len(out) < limit
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}

// findDeadTask locates a dead task by ID.
func (q *NATSWorkQueue) findDeadTask(ctx context.Context, queue string, taskID string) (*deadEntry, error) {

	var found *deadEntry

	err := q.scanDeadTasks(ctx, queue, func(e deadEntry) bool {
		if e.task.ID == taskID {
			found = &e
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	if found == nil {
		return nil, workqueue.ErrTaskNotFound
	}
	return found, nil
}

// RequeueDeadTask moves a dead task back to the pending queue with its
// attempt counter reset.
func (q *NATSWorkQueue) RequeueDeadTask(ctx context.Context, queue string, taskID string) error {

	if err := q.ready(); err != nil {
		return err
	}

	entry, err := q.findDeadTask(ctx, queue, taskID)
	if err != nil {
		return err
	}

	task := entry.task
	task.Attempts = 0
	task.LastError = ""
	task.DiedAt = time.Time{}
	task.RunAt = time.Now()

	// Publish first, then delete: a crash in between leaves a duplicate dead
	// entry rather than a lost task.
	if err := q.publishTask(ctx, q.tasksSubject(queue), task, ""); err != nil {
		return err
	}

	return q.deleteDeadSeq(ctx, queue, entry.seq)
}

// DeleteDeadTask permanently removes a dead task.
func (q *NATSWorkQueue) DeleteDeadTask(ctx context.Context, queue string, taskID string) error {

	if err := q.ready(); err != nil {
		return err
	}

	entry, err := q.findDeadTask(ctx, queue, taskID)
	if err != nil {
		return err
	}

	return q.deleteDeadSeq(ctx, queue, entry.seq)
}

func (q *NATSWorkQueue) deleteDeadSeq(ctx context.Context, queue string, seq uint64) error {

	stream, err := q.js.Stream(ctx, q.dlqStreamName(queue))
	if err != nil {
		return err
	}

	if err := stream.DeleteMsg(ctx, seq); err != nil {
		if errors.Is(err, jetstream.ErrMsgDeleteUnsuccessful) || errors.Is(err, jetstream.ErrMsgNotFound) {
			return workqueue.ErrTaskNotFound
		}
		return err
	}
	return nil
}

// subscription implements workqueue.Subscription for the JetStream backend.
type subscription struct {
	consCtx jetstream.ConsumeContext

	stopOnce sync.Once
	stopCh   chan struct{}
	done     chan struct{}
	sem      chan struct{}
	wg       sync.WaitGroup
}

func (s *subscription) Stop(ctx context.Context) error {
	s.stopOnce.Do(func() {
		close(s.stopCh)
		s.consCtx.Stop()
		go func() {
			// Wait for the dispatch goroutine to finish (no more callbacks),
			// then for in-flight handlers.
			<-s.consCtx.Closed()
			s.wg.Wait()
			close(s.done)
		}()
	})

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
