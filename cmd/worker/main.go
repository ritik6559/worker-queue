package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ritik6559/worker-queue/internal/worker"
)

func main() {
	brokerURL := flag.String("broker", "http://localhost:8080", "broker address")
	logPath := flag.String("log-file", "./jobs.log", "file to append tasks to")
	count := flag.Int("concurrency", 3, "how many workers to run")
	maxWaitMS := flag.Int("wait-ms", 20000, "how long to wait for a task")
	holdForMS := flag.Int("lease-ms", 30000, "how long to claim a task for")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sink, err := worker.NewLogSink(*logPath)
	if err != nil {
		log.Fatalf("could not open log file: %v", err)	
	}	

	client := worker.NewClient(*brokerURL, time.Duration(*maxWaitMS)*time.Millisecond)

	log.Printf("%d workers against %s, appending to %s", *count, *brokerURL, *logPath)

	worker.Run(ctx, client, sink, &worker.Config{
		Count: *count,
		MaxWait: time.Duration(*maxWaitMS) * time.Millisecond,
		HoldFor: time.Duration(*holdForMS) * time.Millisecond,
		RetryPause: time.Second,
	})

	// Run returned, so every worker has stopped and nobody is still writing.
	// Only now is it safe to close the sink.
	log.Println("workers stopped, flushing log file")
	if err := sink.Close(); err != nil {
		log.Printf("could not flush log file: %v", err)
	}
}