//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/BenMz/hiring-pipeline/internal/testkafka"
	"github.com/BenMz/hiring-pipeline/internal/testpg"
)

var (
	broker  string
	adminDB string
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	var wg sync.WaitGroup
	var kafkaErr, pgErr error
	var stopKafka, stopPG func()
	wg.Add(2)
	go func() { defer wg.Done(); broker, stopKafka, kafkaErr = testkafka.Start(ctx) }()
	go func() { defer wg.Done(); adminDB, stopPG, pgErr = testpg.Start(ctx) }()
	wg.Wait()
	if kafkaErr != nil || pgErr != nil {
		fmt.Fprintln(os.Stderr, kafkaErr, pgErr)
		for _, stop := range []func(){stopKafka, stopPG} {
			if stop != nil {
				stop()
			}
		}
		os.Exit(1)
	}
	code := m.Run()
	stopKafka()
	stopPG()
	os.Exit(code)
}
