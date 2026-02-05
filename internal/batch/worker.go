package batch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Worker struct {
	pool       *pgxpool.Pool
	log        *slog.Logger
	input      <-chan InsertRecord
	buffer     []InsertRecord
	maxBatch   int
	flushEvery time.Duration
	columns    []string
}

type WorkerOpts struct {
	Pool       *pgxpool.Pool
	Log        *slog.Logger
	Input      <-chan InsertRecord
	MaxBatch   int
	FlushEvery time.Duration
}

// NewWorker creates a batch worker
func NewWorker(opts WorkerOpts) *Worker {
	cols := MustLoadSQL("cols.sql") // list of columns
	return &Worker{
		pool:       opts.Pool,
		log:        opts.Log,
		input:      opts.Input,
		maxBatch:   opts.MaxBatch,
		flushEvery: opts.FlushEvery,
		columns:    parseColumns(cols),
		buffer:     make([]InsertRecord, 0, opts.MaxBatch),
	}
}

func parseColumns(s string) []string {
	lines := strings.Split(s, ",")
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		c := strings.TrimSpace(l)
		if c != "" {
			out = append(out, c)
		}
	}
	return out
}

// Run starts the batch loop
func (w *Worker) Run(stop <-chan struct{}) {
	w.log.Info("Batch worker started")

	ticker := time.NewTicker(w.flushEvery)
	defer ticker.Stop()

	for {
		select {
		case rec := <-w.input:
			w.buffer = append(w.buffer, rec)

			if len(w.buffer) >= w.maxBatch {
				w.flush()
			}

		case <-ticker.C:
			if len(w.buffer) > 0 {
				w.flush()
			}

		case <-stop:
			w.log.Info("Batch worker stopping...")
			if len(w.buffer) > 0 {
				w.flush()
			}
			return
		}
	}
}

func (w *Worker) flush() {
	if len(w.buffer) == 0 {
		return
	}

	ctx := context.Background()
	var b strings.Builder

	b.WriteString("INSERT INTO k8s_events (")
	b.WriteString(strings.Join(w.columns, ", "))
	b.WriteString(") VALUES ")

	vals := make([]any, 0, len(w.buffer)*len(w.columns))
	for i, r := range w.buffer {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(")
		for j := range w.columns {
			if j > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "$%d", i*len(w.columns)+j+1)
		}
		b.WriteString(")")

		vals = append(vals,
			r.CreatedAt, r.ClusterName, r.UID, r.GlobalUID,
			r.Namespace, r.ResourceKind, r.ResourceName,
			r.EventType, r.Reason, r.Message, r.CompositionID,
			r.Raw, r.ResourceVersion,
		)
	}

	// ON CONFLICT clause for deduplication
	// b.WriteString(" ON CONFLICT (uid, resource_version) DO NOTHING")

	sql := b.String()

	_, err := w.pool.Exec(ctx, sql, vals...)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			// Duplicate key, ignora o logga
			w.log.Debug("duplicate key ignored", slog.Any("err", pgErr))
		} else {
			w.log.Error("batch insert failed", slog.Any("err", err))
		}
	}

	w.log.Debug("flushed batch", slog.Int("count", len(w.buffer)))

	w.buffer = w.buffer[:0] // reset buffer
}
