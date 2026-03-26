package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/krateoplatformops/events-ingester/internal/batch"
	"github.com/krateoplatformops/events-ingester/internal/config"
	"github.com/krateoplatformops/events-ingester/internal/queue"
	"github.com/krateoplatformops/events-ingester/internal/router"
	"github.com/krateoplatformops/events-ingester/internal/telemetry"
	"github.com/krateoplatformops/plumbing/kubeutil"
	"github.com/krateoplatformops/plumbing/pgutil"
	"github.com/krateoplatformops/plumbing/server/probes"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func main() {
	cfg := config.Setup()

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	metrics, shutdownMetrics, err := telemetry.Setup(rootCtx, cfg.Log, telemetry.Config{
		Enabled:        cfg.OTelEnabled,
		ServiceName:    "events-ingester",
		ExportInterval: cfg.OTelExportIntv,
	})
	if err != nil {
		cfg.Log.Error("cannot initialize OpenTelemetry metrics", slog.Any("err", err))
		os.Exit(1)
	}
	defer func() {
		if err := shutdownMetrics(context.Background()); err != nil {
			cfg.Log.Error("OpenTelemetry metrics shutdown failed", slog.Any("err", err))
		}
	}()

	router.StartCacheCleaner(rootCtx, 2*time.Minute)

	pgCtx, cancel := context.WithTimeout(rootCtx, cfg.DbReadyTimeout)
	defer cancel()

	pool, err := pgutil.WaitForPostgres(pgCtx, cfg.Log, cfg.DbURL)
	if err != nil {
		cfg.Log.Error("cannot connect to PostgreSQL", slog.Any("err", err))
		os.Exit(1)
	}
	defer pool.Close()
	cfg.Log.Info("PostgreSQL is ready.")

	// Health probes server
	hs := probes.New(cfg.Log, pool, cfg.Port)
	hs.Start()

	restConfig, err := rest.InClusterConfig()
	if err != nil {
		cfg.Log.Error("cannot get in-cluster config", slog.Any("err", err))
		os.Exit(1)
	}

	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		cfg.Log.Error("cannot create k8s client", slog.Any("err", err))
		os.Exit(1)
	}

	clusterName := kubeutil.DetectClusterName(restConfig)
	cfg.Log.Info("cluster name detected", slog.String("cluster", clusterName))

	// Record channel and batch worker
	recordChan := make(chan batch.InsertRecord, 100)
	batchWorker := batch.NewWorker(batch.WorkerOpts{
		Pool:       pool,
		Log:        cfg.Log,
		Input:      recordChan,
		MaxBatch:   50,
		FlushEvery: 5 * time.Second,
		Metrics:    metrics,
	})
	go batchWorker.Run(rootCtx.Done())

	// Queue with worker pool
	jobQueue := queue.NewQueue(1000, 4)
	jobQueue.Run()
	defer jobQueue.Terminate()

	// Ingester
	ing, err := router.NewIngester(router.IngesterOpts{
		RESTConfig:  restConfig,
		Queue:       jobQueue,
		Pool:        pool,
		Log:         cfg.Log,
		RecordChan:  recordChan,
		ClusterName: clusterName,
		Metrics:     metrics,
	})
	if err != nil {
		cfg.Log.Error("cannot create ingester", slog.Any("err", err))
		os.Exit(1)
	}

	// EventRouter
	evRouter := router.NewEventRouter(router.EventRouterOpts{
		RESTClient:     client.CoreV1().RESTClient(),
		Log:            cfg.Log,
		Handler:        ing,
		ResyncInterval: 30 * time.Second, // TODO make configurable
		ThrottlePeriod: 5 * time.Minute,  // TODO make configurable
		Namespaces:     cfg.Namespaces,
		Metrics:        metrics,
	})
	go evRouter.Run(rootCtx.Done())

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-rootCtx.Done():
				return
			case <-ticker.C:
				metrics.SetRecordChannelDepth(int64(len(recordChan)))
				metrics.SetQueueDepth(int64(jobQueue.GetJobCount()))
			}
		}
	}()

	// Monitor buffer
	go func() {
		ticker := time.NewTicker(50 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-rootCtx.Done():
				return
			case <-ticker.C:
				cfg.Log.Info("Pipeline status",
					slog.Int("recordChan", len(recordChan)),
					slog.Int("queueJobs", jobQueue.GetJobCount()),
				)
			}
		}
	}()

	cfg.Log.Info("Event ingester started")

	<-rootCtx.Done()
	cfg.Log.Info("Shutting down Event ingester")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := hs.Shutdown(shutdownCtx); err != nil {
		cfg.Log.Error("Health server shutdown failed", slog.Any("err", err))
	}
}
