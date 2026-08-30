package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ritik6559/worker-queue/internal/broker"
	"github.com/ritik6559/worker-queue/internal/store/memory"
)

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tasks := memory.New()
	go tasks.Sweep(ctx)

	server := &http.Server{
		Addr:              *addr,
		Handler:           broker.NewServer(tasks).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      broker.LongestWait + 15*time.Second,
	}

	go func() {
		log.Printf("broker listening on %s", *addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("could not listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("shutting down")

	shutDownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutDownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
