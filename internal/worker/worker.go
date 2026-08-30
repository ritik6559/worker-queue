package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/ritik6559/worker-queue/internal/store"
)

type Config struct {
	Count      int
	MaxWait    time.Duration
	HoldFor    time.Duration
	RetryPause time.Duration
}

func Run(ctx context.Context, client *Client, sink *LogSink, config *Config) {
	var running sync.WaitGroup

	for number := 1; number <= config.Count; number++ {
		running.Add(1)
		go func ()  {
			defer running.Done()
			workUntilDone(ctx, client, sink, config, number)
		} ()
	}

	running.Wait()
}

func workUntilDone(ctx context.Context, client *Client, sink *LogSink, config *Config, number int) {
	for {
		if ctx.Err() != nil {
			return
		}

		delivery, err := client.Next(ctx, config.MaxWait, config.HoldFor)
		
		switch {
		case errors.Is(err, ErrNoTasks):
			continue
		case ctx.Err() != nil:
			return
		case err != nil:
			log.Printf("worker %d: could not fetch: %v", number, err)
			pause(ctx, config.RetryPause)
			continue
		}

		// Do the work with a context that survives shutdown, so a task already
		// in hand gets finished and acked rather than abandoned.
		finish, done := context.WithTimeout(context.WithoutCancel(ctx), config.HoldFor)

		if err := handle(sink, delivery); err != nil {
			log.Printf("worker %d: task %s failed: %v", number, delivery.Task.ID, err)
			if err := client.Nack(ctx, delivery.Task.ID, delivery.LeaseID, err.Error()); err != nil {
				log.Printf("worker %d: could not nack %s: %v", number, delivery.Task.ID, err)
			}
		} else if err := client.Ack(finish, delivery.Task.ID, delivery.LeaseID); err != nil {
			log.Printf("worker %d: could not ack %s: %v", number, delivery.Task.ID, err)
		}

		done()
	}
}

func handle(sink *LogSink, delivery *store.Delivery) error {
	if shouldFail(delivery.Task.Payload) {
		return errors.New("payload asked to fail")
	}

	sink.Append(logLine{
		At:      time.Now().UTC(),
		TaskID:  delivery.Task.ID,
		Attempt: delivery.Task.Attempts,
		Payload: delivery.Task.Payload,
	})

	return nil
}

func shouldFail(payload json.RawMessage) bool {
	var fields struct {
		Fail bool `json:"fail"`
	}
	if err := json.Unmarshal(payload, &fields); err != nil {
		return false
	}

	return fields.Fail
}

func pause(ctx context.Context, howLong time.Duration) {
	timer := time.NewTimer(howLong)
	defer timer.Stop()

	select{
	case <-timer.C:
	case <-ctx.Done():
	}
}