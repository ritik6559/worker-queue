package broker

import (
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/ritik6559/worker-queue/internal/store"
	"github.com/ritik6559/worker-queue/internal/task"
)

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	var request enqueueRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "could not read request: "+err.Error())
		return
	}
	if len(request.Payload) == 0 {
		writeError(w, http.StatusBadRequest, "payload is required")
		return
	}

	newTask := task.New(
		request.Payload,
		request.MaxAttempts,
		time.Duration(request.DelayMS)*time.Millisecond,
	)

	if err := s.tasks.Enqueue(newTask); err != nil {
		writeError(w, statusForStoreError(err), err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, enqueueResponse{ID: newTask.ID})
}

// This is the long-poll: it holds the connection open until a task turns up
func (s *Server) handleNext(w http.ResponseWriter, r *http.Request) {
	maxWait := durationParam(r, "wait_ms", DefaultWait, LongestWait)
	holdFor := durationParam(r, "lease_ms", store.DefaultHoldTime, LongestHold)

	delivery, err := s.tasks.Dequeue(r.Context(), maxWait, holdFor)

	switch {
	case errors.Is(err, store.ErrNoTasks):
		w.WriteHeader(http.StatusNoContent)
		return
	case callerLeft(err):
		return
	case err != nil:
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, delivery)
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("id")
	leaseID := r.URL.Query().Get("lease_id")

	if leaseID == "" {
		writeError(w, http.StatusBadRequest, "lease_id is required")
		return
	}

	if err := s.tasks.Ack(taskID, leaseID); err != nil {
		writeError(w, statusForStoreError(err), err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleNack(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("id")

	leaseID := r.URL.Query().Get("lease_id")
	if leaseID == "" {
		writeError(w, http.StatusBadRequest, "lease_id is required")
		return
	}

	var request nackRequest
	if err := decodeJSON(w, r, &request); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "could not read request: "+err.Error())
		return
	}

	reason := request.Error
	if reason == "" {
		reason = "worker reported a failure"
	}

	if err := s.tasks.Nack(taskID, leaseID, reason); err != nil {
		writeError(w, statusForStoreError(err), err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleDeadLetters(w http.ResponseWriter, r *http.Request) {
	buried := s.tasks.DeadTasks()
	if buried == nil {
		buried = []*task.Task{}
	}

	writeJSON(w, http.StatusOK, buried)
}

func (s *Server) handleRequeue(w http.ResponseWriter, r *http.Request) {
	if err := s.tasks.RequeueDead(r.PathValue("id")); err != nil {
		writeError(w, statusForStoreError(err), err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, statsResponse{
		Ready:     s.tasks.ReadyCount(),
		HandedOut: s.tasks.HandedOutCount(),
		Delayed:   s.tasks.DelayedCount(),
		Dead:      s.tasks.DeadCount(),
		Totals:    s.tasks.Totals(),
	})
}
