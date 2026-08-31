package memory

import (
	"context"
	"time"

	"github.com/ritik6559/worker-queue/internal/task"
)

const SweepEvery = 250 * time.Millisecond

func (s *Store) Sweep(ctx context.Context) {
	ticker := time.NewTicker(SweepEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.sweepOnce()
		case <-ctx.Done():
			return
		}
	}
}

func (s *Store) sweepOnce() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.reclaimExpiredHolds()
	s.promoteDueTasks()
}

func (s *Store) reclaimExpiredHolds() {
	now := time.Now().UTC()

	for taskID, held := range s.handedOut {
		if held.deadline.After(now) {
			continue
		}
		delete(s.handedOut, taskID)
		s.counters.Reclaimed.Add(1)
		s.retryOrDelay(held.item, "worker stopped responding")
	}
}

func (s *Store) promoteDueTasks() {
	if len(s.delayed) == 0 {
		return
	}

	now := time.Now().UTC()
	var stillWaiting []*task.Task
	promoted := 0

	for _, waiting := range s.delayed {
		if waiting.AvailableAt.After(now) {
			stillWaiting = append(stillWaiting, waiting)
			continue
		}
		s.ready = append(s.ready, waiting)
		promoted++
	}
	s.delayed = stillWaiting

	if promoted > 0 {
		s.wakeAll()
	}
}
