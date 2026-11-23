// Package app wires the command service together. It is separate from main so
// integration tests can run the real service in-process.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	pipelinev1 "github.com/BenMz/hiring-pipeline/gen/go/pipeline/v1"
	"github.com/BenMz/hiring-pipeline/services/command/internal/eventstore"
	"github.com/BenMz/hiring-pipeline/services/command/internal/server"
)

// Config configures the command service.
type Config struct {
	Brokers []string
	Topic   string
	// RequestTimeout caps how long any single request may run.
	RequestTimeout time.Duration
	// StartupTimeout bounds the initial replay of the topic.
	StartupTimeout time.Duration
}

// ConfigFromEnv reads the config and listen address from the environment.
func ConfigFromEnv() (cfg Config, addr string, err error) {
	cfg = Config{
		Brokers:        strings.Split(env("KAFKA_BROKERS", "localhost:19092"), ","),
		Topic:          env("EVENTS_TOPIC", "applications.events"),
		RequestTimeout: 5 * time.Second,
		StartupTimeout: 60 * time.Second,
	}
	if v := os.Getenv("REQUEST_TIMEOUT"); v != "" {
		if cfg.RequestTimeout, err = time.ParseDuration(v); err != nil {
			return cfg, "", fmt.Errorf("REQUEST_TIMEOUT: %w", err)
		}
	}
	return cfg, env("GRPC_ADDR", ":50051"), nil
}

// Run serves the command API on lis until ctx is cancelled, then shuts down
// gracefully.
func Run(ctx context.Context, cfg Config, lis net.Listener, log *slog.Logger) error {
	startCtx, cancel := context.WithTimeout(ctx, cfg.StartupTimeout)
	defer cancel()
	start := time.Now()
	store, err := eventstore.OpenKafka(startCtx, cfg.Brokers, cfg.Topic, log)
	if err != nil {
		return err
	}
	defer store.Close()
	log.Info("event store ready", "topic", cfg.Topic, "replay", time.Since(start).String())

	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(
		server.Logging(log),
		server.Deadline(cfg.RequestTimeout),
	))
	pipelinev1.RegisterPipelineCommandServiceServer(srv, server.New(store, time.Now, log))
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(srv, healthSrv)
	reflection.Register(srv)

	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(lis) }()
	log.Info("serving", "addr", lis.Addr().String())

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	healthSrv.Shutdown()
	stopped := make(chan struct{})
	go func() { srv.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(cfg.RequestTimeout + time.Second):
		srv.Stop()
	}
	return nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
