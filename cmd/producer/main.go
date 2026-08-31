package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type enqueueRequest struct {
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts int             `json:"max_attempts"`
	DelayMS     int64           `json:"delay_ms"`
}

type enqueueResponse struct {
	ID string `json:"id"`
}

type taskPayload struct {
	Line string `json:"line"`
	At   string `json:"at"`
	Fail bool   `json:"fail,omitempty"`
}

func main() {
	brokerURL := flag.String("broker", "http://localhost:8080", "broker address")
	count := flag.Int("n", 1, "how many tasks to enqueue")
	shouldFail := flag.Bool("fail", false, "enqueue tasks that always fail")
	maxAttempts := flag.Int("max-attempts", 3, "attempts before a task is buried")
	delayMS := flag.Int64("delay-ms", 0, "hold each task back this long before it can be picked up")
	senders := flag.Int("concurrency", 1, "how many tasks to post at once")
	flag.Parse()

	if *count < 1 {
		log.Fatal("-n must be at least 1")
	}
	if *senders < 1 {
		*senders = 1
	}

	client := &http.Client{Timeout: 10 * time.Second}

	taskNumbers := make(chan int)
	var sending sync.WaitGroup
	var enqueued, refused atomic.Int64

	started := time.Now()

	for range *senders {
		sending.Add(1)
		go func() {
			defer sending.Done()

			for number := range taskNumbers {
				id, err := enqueue(client, *brokerURL, enqueueRequest{
					Payload:     buildPayload(number, *shouldFail),
					MaxAttempts: *maxAttempts,
					DelayMS:     *delayMS,
				})
				if err != nil {
					log.Printf("task %d: %v", number, err)
					refused.Add(1)
					continue
				}

				enqueued.Add(1)
				if *count <= 20 {
					fmt.Println(id) // small runs: show the ids so you can curl them
				}
			}
		}()
	}

	for number := 1; number <= *count; number++ {
		taskNumbers <- number
	}
	close(taskNumbers)
	sending.Wait()

	log.Printf("enqueued %d, refused %d, in %s", enqueued.Load(), refused.Load(), time.Since(started).Round(time.Millisecond))
}

func buildPayload(number int, shouldFail bool) json.RawMessage {
	encoded, err := json.Marshal(taskPayload{
		Line: fmt.Sprintf("task %d", number),
		At:   time.Now().UTC().Format(time.RFC3339Nano),
		Fail: shouldFail,
	})
	if err != nil {
		log.Fatalf("could not build payload: %v", err)
	}

	return encoded
}

func enqueue(client *http.Client, brokerURL string, request enqueueRequest) (string, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return "", err
	}

	response, err := client.Post(brokerURL+"/jobs", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, response.Body) // drain so the connection is reusable
		_ = response.Body.Close()
	}()

	if response.StatusCode != http.StatusCreated {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 512))
		return "", fmt.Errorf("broker returned %s: %s", response.Status, bytes.TrimSpace(detail))
	}

	var result enqueueResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("could not read response: %w", err)
	}

	return result.ID, nil
}
