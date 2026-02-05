package pg

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func WaitForPostgres(ctx context.Context, log *slog.Logger, dbURL string) (*pgxpool.Pool, error) {
	var (
		backoff = time.Second      // 1s
		maxBack = 30 * time.Second // limite massimo
	)

	for {
		cfg, err := pgxpool.ParseConfig(dbURL)
		if err != nil {
			return nil, fmt.Errorf("invalid db config: %w", err)
		}

		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err == nil {
			if pingErr := pool.Ping(ctx); pingErr == nil {
				return pool, nil
			}
			pool.Close()
		}

		log.Debug("PostgreSQL not ready yet. Retrying...",
			slog.String("wait", backoff.String()))

		select {
		case <-time.After(backoff):
			// increase backoff, but do not exceed maxBack
			backoff *= 2
			if backoff > maxBack {
				backoff = maxBack
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
