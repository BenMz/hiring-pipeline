//go:build integration

// Package testpg starts a throwaway PostgreSQL for integration tests and
// hands every test its own empty database.
package testpg

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Image is the same PostgreSQL version as docker-compose.yml.
const Image = "postgres:16-alpine"

// Start launches PostgreSQL and returns an admin connection URL.
func Start(ctx context.Context) (adminURL string, stop func(), err error) {
	c, err := postgres.Run(ctx, Image,
		postgres.WithDatabase("pipeline"),
		postgres.WithUsername("pipeline"),
		postgres.WithPassword("pipeline"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		return "", nil, fmt.Errorf("start postgres: %w", err)
	}
	stop = func() { _ = testcontainers.TerminateContainer(c) }
	if adminURL, err = c.ConnectionString(ctx, "sslmode=disable"); err != nil {
		stop()
		return "", nil, err
	}
	return adminURL, stop, nil
}

// FreshDB creates an empty database for the calling test and returns its URL.
// It is dropped when the test ends.
func FreshDB(t *testing.T, adminURL string) string {
	t.Helper()
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, adminURL)
	if err != nil {
		t.Fatal(err)
	}
	name := "t_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		admin.Close()
	})
	u, err := url.Parse(adminURL)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	return u.String()
}

// Pool opens a pool on dbURL that is closed when the test ends.
func Pool(t *testing.T, dbURL string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}
