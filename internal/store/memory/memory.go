package memory

import (
	"context"
	"sync"
	"time"

	"github.com/ritik6559/worker-queue/internal/store"
	"github.com/ritik6559/worker-queue/internal/task"
)

type heldTask struct {
	item     *task.Task
	leaseID  string
	deadline time.Time
}

type Store struct {
	lock      sync.Mutex
	ready     []*task.Task
	delayed   []*task.Task
	dead      []*task.Task
	handedOut map[string]*heldTask
	wakeup    chan struct{}
}

var _ store.Store = (*Store)(nil)

func New() *Store {
	return &Store{
		handedOut: make(map[string]*heldTask),
		wakeup:    make(chan struct{}),
	}
}

func (s *Store) Enqueue(t *task.Task) error {
	if t == nil {
		return store.ErrNilTask
	}
	if t.ID == "" {
		return store.ErrEmptyID
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	s.ready = append(s.ready, t)
	s.wakeAll()

	return nil
}

func (s *Store) Dequeue(ctx context.Context, maxWait, holdFor time.Duration) (*store.Delivery, error) {
	if holdFor <= 0 {
		holdFor = store.DefaultHoldTime
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
			return nil, store.ErrNoTasks
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (s *Store) Ack(taskId, leaseId string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	held, isHeld := s.handedOut[taskId]
	if !isHeld {
		return store.ErrNotHeld
	}
	if held.leaseID != leaseId {
		return store.ErrWrongLease
	}

	delete(s.handedOut, taskId)
	return nil
}

func (s *Store) Nack(taskId, leaseId, reason string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	held, isHeld := s.handedOut[taskId]
	if !isHeld {
		return store.ErrNotHeld
	}
	if held.leaseID != leaseId {
		return store.ErrWrongLease
	}

	delete(s.handedOut, taskId)
	s.retryOrDelay(held.item, reason)

	return nil
}

func (s *Store) retryOrDelay(failed *task.Task, reason string) {
	failed.LastError = reason

	if failed.Attempts >= failed.MaxAttempts {
		s.dead = append(s.dead, failed)
		return
	}

	failed.AvailableAt = time.Now().UTC().Add(store.RetryDelay(failed.Attempts))
	s.delayed = append(s.delayed, failed)
}

func (s *Store) claimOldestTask(holdFor time.Duration) *store.Delivery {
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

	return &store.Delivery{
		Task:     oldest,
		LeaseID:  held.leaseID,
		Deadline: held.deadline,
	}
}

func (s *Store) HandedOutCount() int {
	s.lock.Lock()
	defer s.lock.Unlock()

	return len(s.handedOut)
}

func (s *Store) ReadyCount() int {
	s.lock.Lock()
	defer s.lock.Unlock()

	return len(s.ready)
}

func (s *Store) DelayedCount() int {
	s.lock.Lock()
	defer s.lock.Unlock()

	return len(s.delayed)
}

func (s *Store) DeadCount() int {
	s.lock.Lock()
	defer s.lock.Unlock()

	return len(s.dead)
}

func (s *Store) DeadTasks() []*task.Task {
	s.lock.Lock()
	defer s.lock.Unlock()

	listed := make([]*task.Task, len(s.dead))
	copy(listed, s.dead)

	return listed
}

func (s *Store) wakeAll() {
	close(s.wakeup)
	s.wakeup = make(chan struct{})
}
