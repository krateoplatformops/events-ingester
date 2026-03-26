# Telemetry Assets

This folder contains ready-to-use telemetry assets for `events-ingester`:

- `dashboards/events-ingester-overview.dashboard.json`: Grafana dashboard with example panels
- `collector/otel-collector-config.yaml`: minimal OpenTelemetry Collector config (OTLP HTTP -> Prometheus endpoint)
- `metrics-reference.md`: full metric catalog (type, meaning, examples)

## Prerequisites

1. `events-ingester` running with OpenTelemetry enabled:

```yaml
config:
  OTEL_ENABLED: "true"
  OTEL_EXPORT_INTERVAL: "30s"
  OTEL_EXPORTER_OTLP_ENDPOINT: "http://otel-collector.monitoring.svc.cluster.local:4318"
```

2. OpenTelemetry Collector reachable from `events-ingester`
3. Prometheus scraping the Collector Prometheus exporter (default in this example: `:9464`)
4. Grafana connected to Prometheus as a data source

## Import The Dashboard

1. Open Grafana
2. Go to `Dashboards` -> `New` -> `Import`
3. Upload `dashboards/events-ingester-overview.dashboard.json`
4. Select your Prometheus data source
5. Save

## Example Panels Included

- Received/dispatched/dropped event rates
- Dropped events by reason
- Queue depth and record channel depth
- Composition lookup latency (p50/p95)
- Batch flush latency (p50/p95)
- DB inserted rows rate and DB insert failures

## Metric Naming Notes

Depending on your OTel -> Prometheus conversion rules:

- counters may appear as `<metric>_total`
- histograms usually appear as `<metric>_bucket`, `<metric>_sum`, `<metric>_count`

The provided dashboard uses `or` fallbacks for many counters (for example `metric_total or metric`) to reduce friction.
If your environment still differs, edit panel queries accordingly.

## Collector Example

You can start from `collector/otel-collector-config.yaml` and adapt it to your deployment.

Current pipeline in the example:

- Receiver: OTLP HTTP on `4318`
- Processor: `batch`
- Exporter: Prometheus endpoint on `9464`

## Deploying OpenTelemetry Collector (Helm)

You can deploy a shared Collector in-cluster with the official Helm chart.

1. Add Helm repo:

```bash
helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts
helm repo update
```

2. Create a minimal values file (example `otelcol-values.yaml`):

```yaml
mode: deployment

config:
  receivers:
    otlp:
      protocols:
        http:
          endpoint: 0.0.0.0:4318
  processors:
    batch: {}
  exporters:
    prometheus:
      endpoint: 0.0.0.0:9464
  service:
    pipelines:
      metrics:
        receivers: [otlp]
        processors: [batch]
        exporters: [prometheus]

ports:
  otlp-http:
    enabled: true
    containerPort: 4318
    servicePort: 4318
    protocol: TCP
  prom-metrics:
    enabled: true
    containerPort: 9464
    servicePort: 9464
    protocol: TCP
```

3. Install Collector:

```bash
helm upgrade --install otel-collector open-telemetry/opentelemetry-collector \
  -n monitoring --create-namespace \
  -f otelcol-values.yaml
```

4. Point `events-ingester` to the Collector service:

```yaml
config:
  OTEL_ENABLED: "true"
  OTEL_EXPORTER_OTLP_ENDPOINT: "http://otel-collector-opentelemetry-collector.monitoring.svc.cluster.local:4318"
```

5. Quick checks:

```bash
kubectl -n monitoring get pods
kubectl -n monitoring get svc
```

Then ensure Prometheus scrapes the Collector's `:9464` endpoint.
