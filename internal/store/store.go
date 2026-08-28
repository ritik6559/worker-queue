package store

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/ritik6559/worker-queue/internal/task"
)

var (
	ErrNilTask    = errors.New("store: nil task")
	ErrEmptyID    = errors.New("store: task has no id")
	ErrNoTasks    = errors.New("store: no tasks available")
	ErrNotHeld    = errors.New("store: task is not held by any worker")
	ErrWrongLease = errors.New("store: lease id does not match")
)

const DefaultHoldTime = 30 * time.Second

type Delivery struct {
	Task     *task.Task `json:"task"`
	LeaseID  string     `json:"lease_id"`
	Deadline time.Time  `json:"deadline"`
}

type heldTask struct {
	item     *task.Task
	leaseID  string
	deadline time.Time
}

type Store interface {
	Enqueue(t *task.Task) error
	Dequeue(ctx context.Context, maxWait, holdFor time.Duration) (*Delivery, error)
	Ack(taskId, leaseId string) error
	Nack(taskId, leaseId, reason string) error
}

type MemStore struct {
	lock      sync.Mutex
	ready     []*task.Task
	delayed   []*task.Task
	dead      []*task.Task
	handedOut map[string]*heldTask
	wakeup    chan struct{}
}

func NewMemStore() *MemStore {
	return &MemStore{
		handedOut: make(map[string]*heldTask),
		wakeup:    make(chan struct{}),
	}
}

func (s *MemStore) Enqueue(t *task.Task) error {
	if t == nil {
		return ErrNilTask
	}
	if t.ID == "" {
		return ErrEmptyID
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	s.ready = append(s.ready, t)
	s.wakeAll()

	return nil
}

func (s *MemStore) Dequeue(ctx context.Context, maxWait, holdFor time.Duration) (*Delivery, error) {
	if holdFor <= 0 {
		holdFor = DefaultHoldTime
	}

	giveUp := time.NewTimer(maxWait)
	defer giveUp.Stop()

	for {
		s.lock.Lock()
		if d := s.claimOldestTask(holdFor); d != nil {
			s.lock.Unlock()
			return d, nil
		}
		wakeup := s.wakeup
		s.lock.Unlock()

		select {
		case <-wakeup:
		case <-giveUp.C:
			return nil, ErrNoTasks
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (s *MemStore) Ack(taskId, leaseId string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	held, isHeld := s.handedOut[taskId]
	if !isHeld {
		return ErrNotHeld
	}
	if held.leaseID != leaseId {
		return ErrWrongLease
	}

	delete(s.handedOut, taskId)
	return nil
}

func (s *MemStore) Nack(taskId, leaseId, reason string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	held, isHeld := s.handedOut[taskId]
	if !isHeld {
		return ErrNotHeld
	}
	if held.leaseID != leaseId {
		return ErrWrongLease
	}

	delete(s.handedOut, taskId)
	s.retryOrDelay(held.item, reason)

	return nil
}

func (s *MemStore) retryOrDelay(failed *task.Task, reason string) {
	failed.LastError = reason

	if failed.Attempts >= failed.MaxAttempts {
		s.dead = append(s.dead, failed)
		return
	}

	failed.AvailableAt = time.Now().UTC().Add(retryDelay(failed.Attempts))
	s.delayed = append(s.delayed, failed)
}

func (s *MemStore) claimOldestTask(holdFor time.Duration) *Delivery {
	s.promoteDueTasks()

	if len(s.ready) == 0 {
		return nil
	}

	oldest := s.ready[0]
	s.ready[0] = nil
	s.ready = s.ready[1:]

	oldest.Attempts++

	held := &heldTask{
		item:     oldest,
		leaseID:  task.NewID(),
		deadline: time.Now().UTC().Add(holdFor),
	}
	s.handedOut[oldest.ID] = held

	return &Delivery{
		Task:     oldest,
		LeaseID:  held.leaseID,
		Deadline: held.deadline,
	}
}

func (s *MemStore) promoteDueTasks() {
	if len(s.delayed) == 0 {
		return
	}

	now := time.Now().UTC()
	var stillWaiting []*task.Task

	for _, waiting := range s.delayed {
		if waiting.AvailableAt.After(now) {
			stillWaiting = append(stillWaiting, waiting)
			continue
		}
		s.ready = append(s.ready, waiting)
	}
	s.delayed = stillWaiting

	s.wakeAll()
}

func (s *MemStore) HandedOutCount() int {
	s.lock.Lock()
	defer s.lock.Unlock()

	return len(s.handedOut)
}

func (s *MemStore) ReadyCount() int {
	s.lock.Lock()
	defer s.lock.Unlock()

	return len(s.ready)
}

func (s *MemStore) DelayedCount() int {
	s.lock.Lock()
	defer s.lock.Unlock()

	return len(s.delayed)
}

func (s *MemStore) DeadCount() int {
	s.lock.Lock()
	defer s.lock.Unlock()

	return len(s.dead)
}

func (s *MemStore) DeadTasks() []*task.Task {
	s.lock.Lock()
	defer s.lock.Unlock()

	listed := make([]*task.Task, len(s.dead))
	copy(listed, s.dead)

	return listed
}

func (s *MemStore) wakeAll() {
	close(s.wakeup)
	s.wakeup = make(chan struct{})
}

func retryDelay(attempts int) time.Duration {
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
