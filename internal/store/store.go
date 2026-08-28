package store

import (
	"context"
	"errors"
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
}

type MemStore struct {
	lock      sync.Mutex
	ready     []*task.Task
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

func (s *MemStore) claimOldestTask(holdFor time.Duration) *Delivery {
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

func (s *MemStore) wakeAll() {
	close(s.wakeup)
	s.wakeup = make(chan struct{})
}
