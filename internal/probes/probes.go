package probes

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type HealthServer struct {
	srv *http.Server
	log *slog.Logger
}

// New starts an HTTP health server exposing /livez and /readyz.
//
// The readiness probe requires that the PostgreSQL connection pool is healthy.
// Liveness always returns 200.
func New(log *slog.Logger, pool *pgxpool.Pool, port int) *HealthServer {
	mux := http.NewServeMux()

	// Liveness probe: OK unless the process is stuck.
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Readiness probe: database must be reachable.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()

		if err := pool.Ping(ctx); err != nil {
			log.Error("readiness: db unreachable", slog.Any("err", err))
			http.Error(w, "db unreachable", http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{
		Addr:         ":" + strconv.Itoa(port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 25 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	return &HealthServer{srv: srv, log: log}
}

// Start runs the HTTP server in a background goroutine.
func (h *HealthServer) Start() {
	go func() {
		if err := h.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			h.log.Error("health server error", slog.Any("err", err))
		}
	}()
}

// Shutdown gracefully stops the HTTP server.
func (h *HealthServer) Shutdown(ctx context.Context) error {
	return h.srv.Shutdown(ctx)
}
