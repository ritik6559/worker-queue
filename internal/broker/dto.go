package broker

import "encoding/json"

type enqueueRequest struct {
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts int             `json:"max_attempts"`
	DelayMS     int64           `json:"delay_ms"`
}

type enqueueResponse struct {
	ID string `json:"id"`
}

type nackRequest struct {
	Error string `json:"error"`
}

type statsResponse struct {
	Ready     int `json:"ready"`
	HandedOut int `json:"handed_out"`
	Delayed   int `json:"delayed"`
	Dead      int `json:"dead"`
}

type errorResponse struct {
	Error string `json:"error"`
}
