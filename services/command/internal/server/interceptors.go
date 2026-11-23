package server

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// Deadline gives every request a deadline of at most max. A client deadline
// that is shorter is kept; a missing or longer one is capped, so a stuck
// broker can never hold a request (and its stream lock) forever.
func Deadline(maxTimeout time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if dl, ok := ctx.Deadline(); !ok || time.Until(dl) > maxTimeout {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, maxTimeout)
			defer cancel()
		}
		return handler(ctx, req)
	}
}

// Logging logs one line per request with its status code and duration.
func Logging(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		res, err := handler(ctx, req)
		level := slog.LevelInfo
		if err != nil {
			level = slog.LevelWarn
		}
		log.Log(ctx, level, "rpc", "method", info.FullMethod, "code", status.Code(err).String(), "duration", time.Since(start).String())
		return res, err
	}
}
