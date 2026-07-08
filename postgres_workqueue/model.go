package postgres_workqueue

import (
	"encoding/json"
	"time"

	"github.com/weedbox/workqueue-modules/workqueue"
)

// Task lifecycle states stored in TaskRecord.Status.
const (
	StatusPending = "pending"
	StatusRunning = "running"
	StatusDead    = "dead"
)

// TaskRecord is the database row backing one task. The column layout matches
// gorm_workqueue's table exactly, so both backends can be pointed at the same
// table interchangeably; only the index names differ (PostgreSQL index names
// are global per schema, so each backend uses its own prefix to avoid
// collisions when both are migrated into one database).
type TaskRecord struct {
	ID         string    `gorm:"primaryKey;size:36"`
	Queue      string    `gorm:"size:191;index:idx_pg_workqueue_tasks_claim,priority:1"`
	Status     string    `gorm:"size:16;index:idx_pg_workqueue_tasks_claim,priority:2"`
	RunAt      time.Time `gorm:"index:idx_pg_workqueue_tasks_claim,priority:3"`
	Payload    []byte
	Metadata   []byte // JSON-encoded map[string]string
	Attempts   int
	MaxRetries int
	EnqueuedAt time.Time
	LastError  string
	DiedAt     *time.Time

	// LeaseExpiresAt bounds how long a running task may go unacknowledged
	// before it is handed back to the pending state.
	LeaseExpiresAt *time.Time `gorm:"index:idx_pg_workqueue_tasks_lease"`
}

// toTask converts a database row to the shared task shape.
func (r *TaskRecord) toTask() (*workqueue.Task, error) {

	var metadata map[string]string
	if len(r.Metadata) > 0 {
		if err := json.Unmarshal(r.Metadata, &metadata); err != nil {
			return nil, err
		}
	}

	task := &workqueue.Task{
		ID:         r.ID,
		Queue:      r.Queue,
		Payload:    r.Payload,
		Metadata:   metadata,
		Attempts:   r.Attempts,
		MaxRetries: r.MaxRetries,
		EnqueuedAt: r.EnqueuedAt,
		RunAt:      r.RunAt,
		LastError:  r.LastError,
	}
	if r.DiedAt != nil {
		task.DiedAt = *r.DiedAt
	}

	return task, nil
}
