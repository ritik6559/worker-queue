package broker

import (
	"net/http"
	"time"

	"github.com/ritik6559/worker-queue/internal/task"
)

func (s *Server) handleEnqueue(w http.ResponseWriter, r* http.Request) {
	var request enqueueRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "could not read request: "+err.Error())
	}
	if len(request.Payload) == 0{
		writeError(w, http.StatusBadRequest, "payload is required")
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

