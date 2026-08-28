package broker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/ritik6559/worker-queue/internal/store"
	"github.com/ritik6559/worker-queue/internal/task"
)

const (
	LongestWait  = 30 * time.Second
	LongestHold  = 5 * time.Minute
	DefaultWait  = 20 * time.Second
	maxReadBytes = 1 << 20 // 1 MiB
)

type TaskQueue interface {
	store.Store

	ReadyCount() int
	HandedOutCount() int
	DelayedCount() int
	DeadCount() int
	DeadTasks() []*task.Task
}

type Server struct {
	tasks TaskQueue
}

func NewServer(tasks TaskQueue) *Server {
	return &Server{
		tasks: tasks,
	}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /jobs", s.handleEnqueue)

	return mux
}

func statusForStoreError(err error) int {
	switch {
	case errors.Is(err, store.ErrNotHeld):
		return http.StatusNotFound
	case errors.Is(err, store.ErrWrongLease):
		return http.StatusConflict
	case errors.Is(err, store.ErrNilTask), errors.Is(err, store.ErrEmptyID):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, into any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxReadBytes)
	return json.NewDecoder(r.Body).Decode(into)
}

func durationParam(r *http.Request, name string, fallback, longest time.Duration) time.Duration {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return fallback
	}

	milliseconds, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || milliseconds < 0 {
		return fallback
	}

	asked := time.Duration(milliseconds) * time.Millisecond
	if asked > longest {
		return longest
	}

	return asked
}

func callerLeft(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
