// Command pipeline-projector builds the PostgreSQL read models from the event
// topic.
//
//	pipeline-projector           project continuously (default)
//	pipeline-projector rebuild   drop the read models, replay from offset 0, exit
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/BenMz/hiring-pipeline/services/projector/app"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log, arg(1)); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, cmd string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg := app.ConfigFromEnv()

	switch cmd {
	case "", "run":
		return app.Run(ctx, cfg, log)
	case "rebuild":
		stats, err := app.Rebuild(ctx, cfg, log)
		if err == nil {
			log.Info("rebuild complete", "events", stats.Events, "duration", stats.Duration.String())
		}
		return err
	default:
		return fmt.Errorf("unknown command %q (want run or rebuild)", cmd)
	}
}

func arg(i int) string {
	if len(os.Args) > i {
		return os.Args[i]
	}
	return ""
}
