package task

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"
)

type State string 

const (
	StateReady    State = "ready"
	StateInFlight State = "inflight"
	StateDelayed  State = "delayed"
	StateDead     State = "dead"
)

const DefaultMaxAttempts = 3

type Task struct {
	ID          string          `json:"id"`
	Payload     json.RawMessage `json:"payload"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	CreatedAt   time.Time       `json:"created_at"`
	AvailableAt time.Time       `json:"available_at"`
	LastError   string          `json:"last_error,omitempty"`
}

func New(payload json.RawMessage, maxAttempts int, delay time.Duration) *Task {
	if( maxAttempts <= 0 ) {
		maxAttempts = DefaultMaxAttempts
	}
	now := time.Now().UTC()

	return &Task{
		ID: NewID(),
		Payload: payload,
		Attempts: 0,
		MaxAttempts: maxAttempts,
		CreatedAt: now,
		AvailableAt: now.Add(delay),
	}
}

func NewID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}