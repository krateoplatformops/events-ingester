package telemetry

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

type Config struct {
	Enabled        bool
	ServiceName    string
	ExportInterval time.Duration
}

type Metrics struct {
	eventsReceived          metric.Int64Counter
	eventsDispatched        metric.Int64Counter
	eventsDropped           metric.Int64Counter
	compositionLookupDur    metric.Float64Histogram
	recordBuildFailure      metric.Int64Counter
	batchFlushDuration      metric.Float64Histogram
	batchFlushSize          metric.Int64Histogram
	dbInsertRows            metric.Int64Counter
	dbInsertFailure         metric.Int64Counter
	queueDepthGauge         metric.Int64ObservableGauge
	recordChannelDepthGauge metric.Int64ObservableGauge

	queueDepth         atomic.Int64
	recordChannelDepth atomic.Int64
}

func Setup(ctx context.Context, log *slog.Logger, cfg Config) (*Metrics, func(context.Context) error, error) {
	if !cfg.Enabled {
		return nil, func(context.Context) error { return nil }, nil
	}

	exporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, nil, err
	}

	res, err := resource.Merge(resource.Default(),
		resource.NewSchemaless(attribute.String("service.name", cfg.ServiceName)))
	if err != nil {
		return nil, nil, err
	}

	readerOptions := []sdkmetric.PeriodicReaderOption{}
	if cfg.ExportInterval > 0 {
		readerOptions = append(readerOptions, sdkmetric.WithInterval(cfg.ExportInterval))
	}

	reader := sdkmetric.NewPeriodicReader(exporter, readerOptions...)
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(provider)

	meter := provider.Meter("github.com/krateoplatformops/events-ingester")
	metrics, err := newMetrics(meter)
	if err != nil {
		_ = provider.Shutdown(ctx)
		return nil, nil, err
	}

	log.Info("OpenTelemetry metrics initialized")
	return metrics, provider.Shutdown, nil
}

func newMetrics(meter metric.Meter) (*Metrics, error) {
	var err error
	m := &Metrics{}

	if m.eventsReceived, err = meter.Int64Counter("events_ingester.events.received"); err != nil {
		return nil, err
	}
	if m.eventsDispatched, err = meter.Int64Counter("events_ingester.events.dispatched"); err != nil {
		return nil, err
	}
	if m.eventsDropped, err = meter.Int64Counter("events_ingester.events.dropped"); err != nil {
		return nil, err
	}
	if m.compositionLookupDur, err = meter.Float64Histogram("events_ingester.composition.lookup.duration_seconds"); err != nil {
		return nil, err
	}
	if m.recordBuildFailure, err = meter.Int64Counter("events_ingester.records.build.failure"); err != nil {
		return nil, err
	}
	if m.batchFlushDuration, err = meter.Float64Histogram("events_ingester.batch.flush.duration_seconds"); err != nil {
		return nil, err
	}
	if m.batchFlushSize, err = meter.Int64Histogram("events_ingester.batch.flush.size"); err != nil {
		return nil, err
	}
	if m.dbInsertRows, err = meter.Int64Counter("events_ingester.db.insert.rows"); err != nil {
		return nil, err
	}
	if m.dbInsertFailure, err = meter.Int64Counter("events_ingester.db.insert.failure"); err != nil {
		return nil, err
	}
	if m.queueDepthGauge, err = meter.Int64ObservableGauge("events_ingester.queue.depth"); err != nil {
		return nil, err
	}
	if m.recordChannelDepthGauge, err = meter.Int64ObservableGauge("events_ingester.record_channel.depth"); err != nil {
		return nil, err
	}

	_, err = meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		observer.ObserveInt64(m.queueDepthGauge, m.queueDepth.Load())
		observer.ObserveInt64(m.recordChannelDepthGauge, m.recordChannelDepth.Load())
		return nil
	}, m.queueDepthGauge, m.recordChannelDepthGauge)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (m *Metrics) IncEventsReceived(ctx context.Context) {
	if m == nil {
		return
	}
	m.eventsReceived.Add(ctx, 1)
}

func (m *Metrics) IncEventsDispatched(ctx context.Context) {
	if m == nil {
		return
	}
	m.eventsDispatched.Add(ctx, 1)
}

func (m *Metrics) IncEventsDropped(ctx context.Context, reason string) {
	if m == nil {
		return
	}
	m.eventsDropped.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

func (m *Metrics) RecordCompositionLookupDuration(ctx context.Context, d time.Duration, result string) {
	if m == nil {
		return
	}
	m.compositionLookupDur.Record(ctx, d.Seconds(), metric.WithAttributes(attribute.String("result", result)))
}

func (m *Metrics) IncRecordBuildFailure(ctx context.Context, reason string) {
	if m == nil {
		return
	}
	m.recordBuildFailure.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

func (m *Metrics) RecordBatchFlush(ctx context.Context, d time.Duration, size int) {
	if m == nil {
		return
	}
	m.batchFlushDuration.Record(ctx, d.Seconds())
	if size > 0 {
		m.batchFlushSize.Record(ctx, int64(size))
	}
}

func (m *Metrics) AddDBInsertRows(ctx context.Context, rows int64) {
	if m == nil || rows <= 0 {
		return
	}
	m.dbInsertRows.Add(ctx, rows)
}

func (m *Metrics) IncDBInsertFailure(ctx context.Context, failureType string) {
	if m == nil {
		return
	}
	m.dbInsertFailure.Add(ctx, 1, metric.WithAttributes(attribute.String("type", failureType)))
}

func (m *Metrics) SetQueueDepth(depth int64) {
	if m == nil {
		return
	}
	m.queueDepth.Store(depth)
}

func (m *Metrics) SetRecordChannelDepth(depth int64) {
	if m == nil {
		return
	}
	m.recordChannelDepth.Store(depth)
}
