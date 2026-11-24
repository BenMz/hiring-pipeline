// Package app wires the query service together. It is separate from main so
// integration tests can run the real service in-process.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	pipelinev1 "github.com/BenMz/hiring-pipeline/gen/go/pipeline/v1"
	"github.com/BenMz/hiring-pipeline/internal/grpcx"
	"github.com/BenMz/hiring-pipeline/services/query/internal/server"
)

// Config configures the query service.
type Config struct {
	DatabaseURL    string
	RequestTimeout time.Duration
}

// ConfigFromEnv reads the config and listen address from the environment.
func ConfigFromEnv() (cfg Config, addr string, err error) {
	cfg = Config{
		DatabaseURL:    env("DATABASE_URL", "postgres://pipeline:pipeline@localhost:5432/pipeline?sslmode=disable"),
		RequestTimeout: 2 * time.Second,
	}
	if v := os.Getenv("REQUEST_TIMEOUT"); v != "" {
		if cfg.RequestTimeout, err = time.ParseDuration(v); err != nil {
			return cfg, "", fmt.Errorf("REQUEST_TIMEOUT: %w", err)
		}
	}
	return cfg, env("GRPC_ADDR", ":50052"), nil
}

// Run serves the query API on lis until ctx is cancelled.
func Run(ctx context.Context, cfg Config, lis net.Listener, log *slog.Logger) error {
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(
		grpcx.Logging(log),
		grpcx.Deadline(cfg.RequestTimeout),
	))
	pipelinev1.RegisterPipelineQueryServiceServer(srv, server.New(pool, log))
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
