// Package app wires the projector together. It is separate from main so
// integration tests can run the real projector in-process.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BenMz/hiring-pipeline/services/projector/internal/projection"
	"github.com/BenMz/hiring-pipeline/services/projector/internal/runner"
)

// Config configures the projector.
type Config struct {
	Brokers     []string
	Topic       string
	DatabaseURL string
}

// ConfigFromEnv reads the config from the environment.
func ConfigFromEnv() Config {
	return Config{
		Brokers:     strings.Split(env("KAFKA_BROKERS", "localhost:19092"), ","),
		Topic:       env("EVENTS_TOPIC", "applications.events"),
		DatabaseURL: env("DATABASE_URL", "postgres://pipeline:pipeline@localhost:5432/pipeline?sslmode=disable"),
	}
}

// ErrAlreadyRunning means another projector holds the lock on the read models.
var ErrAlreadyRunning = errors.New("another projector is running against this database; stop it first")

// lockKey identifies the projector's PostgreSQL advisory lock.
const lockKey = 0x70726f6a // "proj"

// Stats describes a catch-up or rebuild run.
type Stats struct {
	Events   int64
	Duration time.Duration
}

// Run applies pending events and then keeps projecting until ctx is cancelled.
func Run(ctx context.Context, cfg Config, log *slog.Logger) error {
	return withProjector(ctx, cfg, log, false, func(r *runner.Runner) error {
		log.Info("projecting", "topic", cfg.Topic)
		return r.Run(ctx)
	})
}

// CatchUp applies every event currently in the topic and returns.
func CatchUp(ctx context.Context, cfg Config, log *slog.Logger) (Stats, error) {
	return catchUp(ctx, cfg, log, false)
}

// Rebuild drops all read models and offsets, replays the topic from offset 0
// and returns once it has caught up.
func Rebuild(ctx context.Context, cfg Config, log *slog.Logger) (Stats, error) {
	return catchUp(ctx, cfg, log, true)
}

func catchUp(ctx context.Context, cfg Config, log *slog.Logger, reset bool) (Stats, error) {
	var stats Stats
	err := withProjector(ctx, cfg, log, reset, func(r *runner.Runner) error {
		start := time.Now()
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- r.Run(runCtx) }()

		waitErr := r.WaitCaughtUp(ctx)
		stop()
		runErr := <-done
		stats = Stats{Events: r.Applied(), Duration: time.Since(start)}
		return errors.Join(runErr, waitErr)
	})
	return stats, err
}

// withProjector takes the advisory lock, prepares the schema and runs fn.
func withProjector(ctx context.Context, cfg Config, log *slog.Logger, reset bool, fn func(*runner.Runner) error) error {
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	// A session-level advisory lock on a dedicated connection: only one
	// projector may write the read models, and a rebuild can't run under a
	// live projector.
	lockConn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer lockConn.Release()
	var locked bool
	if err := lockConn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockKey).Scan(&locked); err != nil {
		return err
	}
	if !locked {
		return ErrAlreadyRunning
	}
	defer func() { _, _ = lockConn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", lockKey) }()

	if reset {
		log.Info("dropping read models for rebuild")
		err = projection.Reset(ctx, pool)
	} else {
		err = projection.Migrate(ctx, pool)
	}
	if err != nil {
		return err
	}

	r, err := runner.New(ctx, pool, cfg.Brokers, cfg.Topic, log)
	if err != nil {
		return err
	}
	defer r.Close()
	return fn(r)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
