package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/ritik6559/worker-queue/internal/store"
)

var ErrNoTasks = errors.New("wroker: no new tasks")

type Client struct {
	brokerURL string
	http      *http.Client
}

func NewClient(brokerURL string, longestWait time.Duration) *Client {
	return &Client{
		brokerURL: brokerURL,
		http: &http.Client{
			Timeout: longestWait + 15 * time.Second,
		},
	}
}

func (c *Client) Next(ctx context.Context, maxWait, holdFor time.Duration) (*store.Delivery, error) {
	query := url.Values{}
	query.Set("wait_ms", strconv.FormatInt(maxWait.Milliseconds(), 10))
	query.Set("lease_ms", strconv.FormatInt(holdFor.Milliseconds(), 10))

	address := c.brokerURL + "/jobs/next?" + query.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return nil, err
	}

	response, err := c.http.Do(request)
	if err != nil {
		return nil, err
	}
	defer closeBody(response)

	switch response.StatusCode{
	case http.StatusNoContent:
		return nil, ErrNoTasks
	case http.StatusOK:
		var delivery store.Delivery
		if err := json.NewDecoder(response.Body).Decode(&delivery); err != nil {
			return nil, fmt.Errorf("could not read delivery: %w", err)
		}
		return &delivery, nil
	default:
		return nil, unexpectedStatus(response)
	}
}

func unexpectedStatus(response *http.Response) error {
	detail, _ := io.ReadAll(io.LimitReader(response.Body, 512))
	return fmt.Errorf("broker returned %s: %s", response.Status, bytes.TrimSpace(detail))
}


func closeBody(response *http.Response) {
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
}