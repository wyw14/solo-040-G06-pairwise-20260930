package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/wyw14/cry-96/internal/api"
)

type options struct {
	address       string
	dataDirectory string
	webDirectory  string
}

func parseOptions() options {
	var value options
	flag.StringVar(&value.address, "address", "127.0.0.1:19696", "HTTP listen address")
	flag.StringVar(&value.dataDirectory, "data", "var/lockflow", "journal data directory")
	flag.StringVar(&value.webDirectory, "web", "web", "web asset directory")
	flag.Parse()
	return value
}

func main() {
	if err := run(); err != nil {
		slog.Error("lockflow stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	configuration := parseOptions()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runtime, err := api.NewRuntime(ctx, configuration.dataDirectory)
	if err != nil {
		return err
	}
	// Project safety events into notifications and retry pending deliveries.
	// The loop is deliberately independent of HTTP serving: delivery failures
	// are retried with backoff and can never roll back an emergency stop or plan
	// cancellation that has already been journaled.
	go runtime.NotifyOutbox.Run(ctx, 2*time.Second)
	server := &http.Server{
		Addr: configuration.address, Handler: api.Router(runtime, configuration.webDirectory),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
	}
	serveErrors := make(chan error, 1)
	go func() {
		slog.Info("lockflow listening", "address", configuration.address)
		serveErrors <- server.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownContext)
	case err := <-serveErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
