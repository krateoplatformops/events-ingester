# Events-Ingester Metrics Reference

This document describes the OpenTelemetry metrics emitted by `events-ingester`.

## Naming note

In code, metric names use dots (for example `events_ingester.events.received`).
In Prometheus, names are typically normalized with underscores (for example `events_ingester_events_received`), and counters may be exposed with `_total`.

## Metrics

| Metric | Type | Unit | Description | Labels | Emitted from | PromQL example |
|---|---|---|---|---|---|---|
| `events_ingester.events.received` | Counter | count | Number of events accepted by router input path. | none | `internal/router/router.go` | `sum(rate(events_ingester_events_received_total[5m])) or sum(rate(events_ingester_events_received[5m]))` |
| `events_ingester.events.dispatched` | Counter | count | Number of events forwarded to the ingester handler. | none | `internal/router/router.go` | `sum(rate(events_ingester_events_dispatched_total[5m])) or sum(rate(events_ingester_events_dispatched[5m]))` |
| `events_ingester.events.dropped` | Counter | count | Dropped events in router/ingester paths. | `reason` | `internal/router/router.go`, `internal/router/handler.go` | `sum by (reason) (rate(events_ingester_events_dropped_total[5m])) or sum by (reason) (rate(events_ingester_events_dropped[5m]))` |
| `events_ingester.composition.lookup.duration_seconds` | Histogram | seconds | Time spent resolving composition ID for an involved object. | `result` | `internal/router/handler.go` | `histogram_quantile(0.95, sum by (le) (rate(events_ingester_composition_lookup_duration_seconds_bucket[5m])))` |
| `events_ingester.records.build.failure` | Counter | count | Failures while building DB records from events. | `reason` | `internal/router/handler.go` | `sum by (reason) (increase(events_ingester_records_build_failure_total[1h])) or sum by (reason) (increase(events_ingester_records_build_failure[1h]))` |
| `events_ingester.batch.flush.duration_seconds` | Histogram | seconds | Duration of a batch flush cycle. | none | `internal/batch/worker.go` | `histogram_quantile(0.95, sum by (le) (rate(events_ingester_batch_flush_duration_seconds_bucket[5m])))` |
| `events_ingester.batch.flush.size` | Histogram | records | Number of records per flush. | none | `internal/batch/worker.go` | `histogram_quantile(0.95, sum by (le) (rate(events_ingester_batch_flush_size_bucket[5m])))` |
| `events_ingester.db.insert.rows` | Counter | rows | Number of rows inserted by successful batch writes. | none | `internal/batch/worker.go` | `sum(rate(events_ingester_db_insert_rows_total[5m])) or sum(rate(events_ingester_db_insert_rows[5m]))` |
| `events_ingester.db.insert.failure` | Counter | count | Batch insert failures. | `type` | `internal/batch/worker.go` | `sum by (type) (increase(events_ingester_db_insert_failure_total[1h])) or sum by (type) (increase(events_ingester_db_insert_failure[1h]))` |
| `events_ingester.queue.depth` | Gauge | count | In-memory queue buffered job count. | none | `main.go` | `max(events_ingester_queue_depth)` |
| `events_ingester.record_channel.depth` | Gauge | count | In-memory record channel buffered item count. | none | `main.go` | `max(events_ingester_record_channel_depth)` |

## Label cardinality guidance

- Keep labels low-cardinality and bounded (`reason`, `result`, `type`).
- Avoid dynamic labels such as `uid`, `resource_name`, `message`, `composition_id`.
- Prefer service-level observability over per-event dimensions.
