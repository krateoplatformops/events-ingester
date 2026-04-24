package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var partitionBoundPattern = regexp.MustCompile(`FOR VALUES FROM \('([^']+)'\) TO \('([^']+)'\)`)

type PartitionRange struct {
	MinPartitionStart time.Time
	MaxPartitionEnd   time.Time
}

func (r PartitionRange) Contains(ts time.Time) bool {
	return !ts.Before(r.MinPartitionStart) && ts.Before(r.MaxPartitionEnd)
}

type PartitionRangeProvider interface {
	Current(ctx context.Context) (PartitionRange, bool)
}

type cachedPartitionRangeProvider struct {
	pool         *pgxpool.Pool
	log          *slog.Logger
	refreshEvery time.Duration

	mu          sync.RWMutex
	cachedRange PartitionRange
	cached      bool
	refreshedAt time.Time
}

func NewPartitionRangeProvider(pool *pgxpool.Pool, log *slog.Logger, refreshEvery time.Duration) PartitionRangeProvider {
	if refreshEvery <= 0 {
		refreshEvery = time.Minute
	}

	return &cachedPartitionRangeProvider{
		pool:         pool,
		log:          log,
		refreshEvery: refreshEvery,
	}
}

func (p *cachedPartitionRangeProvider) Current(ctx context.Context) (PartitionRange, bool) {
	p.mu.RLock()
	if p.cached && time.Since(p.refreshedAt) < p.refreshEvery {
		rng := p.cachedRange
		p.mu.RUnlock()
		return rng, true
	}
	p.mu.RUnlock()

	rng, err := loadPartitionRange(ctx, p.pool)
	if err != nil {
		p.mu.RLock()
		cachedRange := p.cachedRange
		cached := p.cached
		p.mu.RUnlock()

		if cached {
			p.log.Warn("unable to refresh k8s_events partition range, using cached range",
				slog.Any("err", err),
				slog.String("minPartitionStart", cachedRange.MinPartitionStart.Format(time.RFC3339Nano)),
				slog.String("maxPartitionEnd", cachedRange.MaxPartitionEnd.Format(time.RFC3339Nano)),
			)
			return cachedRange, true
		}

		p.log.Warn("k8s_events partition range unavailable, outside-range filter disabled",
			slog.Any("err", err),
		)
		return PartitionRange{}, false
	}

	p.mu.Lock()
	p.cachedRange = rng
	p.cached = true
	p.refreshedAt = time.Now()
	p.mu.Unlock()

	return rng, true
}

func loadPartitionRange(ctx context.Context, pool *pgxpool.Pool) (PartitionRange, error) {
	rows, err := pool.Query(ctx, `
SELECT pg_get_expr(child.relpartbound, child.oid, false) AS partition_bound
FROM pg_inherits inh
JOIN pg_class parent ON parent.oid = inh.inhparent
JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
JOIN pg_class child ON child.oid = inh.inhrelid
WHERE parent.relname = 'k8s_events'
  AND parent_ns.nspname = ANY(current_schemas(false));
`)
	if err != nil {
		return PartitionRange{}, err
	}
	defer rows.Close()

	var (
		minStart time.Time
		maxEnd   time.Time
		found    bool
	)

	for rows.Next() {
		var bound string
		if err := rows.Scan(&bound); err != nil {
			return PartitionRange{}, err
		}

		start, end, ok, err := parsePartitionBound(bound)
		if err != nil {
			return PartitionRange{}, err
		}
		if !ok {
			continue
		}

		if !found || start.Before(minStart) {
			minStart = start
		}
		if !found || end.After(maxEnd) {
			maxEnd = end
		}
		found = true
	}

	if err := rows.Err(); err != nil {
		return PartitionRange{}, err
	}
	if !found {
		return PartitionRange{}, errors.New("no concrete k8s_events partitions found")
	}

	return PartitionRange{
		MinPartitionStart: minStart.UTC(),
		MaxPartitionEnd:   maxEnd.UTC(),
	}, nil
}

func parsePartitionBound(bound string) (time.Time, time.Time, bool, error) {
	matches := partitionBoundPattern.FindStringSubmatch(bound)
	if len(matches) != 3 {
		return time.Time{}, time.Time{}, false, nil
	}

	start, err := parsePartitionTime(matches[1])
	if err != nil {
		return time.Time{}, time.Time{}, false, fmt.Errorf("parse partition start %q: %w", matches[1], err)
	}

	end, err := parsePartitionTime(matches[2])
	if err != nil {
		return time.Time{}, time.Time{}, false, fmt.Errorf("parse partition end %q: %w", matches[2], err)
	}

	return start, end, true, nil
}

func parsePartitionTime(raw string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999-07",
		"2006-01-02 15:04:05.999999-07",
		"2006-01-02 15:04:05-07",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, layout := range layouts {
		ts, err := time.Parse(layout, raw)
		if err == nil {
			return ts.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("unsupported partition time format: %s", raw)
}
