package store

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/ritik6559/worker-queue/internal/task"
)

var (
	ErrNilTask    = errors.New("store: nil task")
	ErrEmptyID    = errors.New("store: task has no id")
	ErrNoTasks    = errors.New("store: no tasks available")
	ErrNotHeld    = errors.New("store: task is not held by any worker")
	ErrWrongLease = errors.New("store: lease id does not match")
	ErrNotBuried  = errors.New("store: task is not in the dead-letter queue")
)

const DefaultHoldTime = 30 * time.Second

type Delivery struct {
	Task     *task.Task `json:"task"`
	LeaseID  string     `json:"lease_id"`
	Deadline time.Time  `json:"deadline"`
}

type Store interface {
	Enqueue(t *task.Task) error
	Dequeue(ctx context.Context, maxWait, holdFor time.Duration) (*Delivery, error)
	Ack(taskId, leaseId string) error
	Nack(taskId, leaseId, reason string) error
}

func RetryDelay(attempts int) time.Duration {
	const fireDelay = time.Second
	const longestDelay = 30 * time.Second

	if attempts < 1 {
		attempts = 1
	}
	if attempts > 10 {
		attempts = 10
	}

	delay := fireDelay << (attempts - 1)
	if delay > longestDelay {
		delay = longestDelay
	}

	// Wait at least half that, plus a random slice of the other half, so a
	// batch of tasks that failed together don't all retry at the same instant
	// and knock over whatever they were failing against.
	half := delay / 2
	return half + time.Duration(rand.Int64N(int64(half)))
}
