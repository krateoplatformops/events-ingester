# events-ingester

**events-ingester** is a Kubernetes-native service that captures cluster events, enriches them with metadata, and persists them into PostgreSQL for downstream processing and real-time delivery.

It is typically deployed alongside **events-presenter**, forming a complete event pipeline:

```txt
Kubernetes Events → events-ingester → PostgreSQL → events-presenter → Clients
```

The ingester focuses on **collection, normalization, and storage** of events at scale.

## What Does It Do?

The service continuously watches Kubernetes events across configured namespaces and transforms them into structured records stored in PostgreSQL.

Core responsibilities:

- Watch cluster events using the in-cluster Kubernetes client
- Resolve related objects (for example composition IDs)
- Enrich events with cluster metadata
- Batch inserts for high performance
- Push records into PostgreSQL
- Enable downstream streaming via LISTEN/NOTIFY
- Provide health probes for Kubernetes
- Handle graceful shutdown


## Architecture Overview

```
Kubernetes API
     ↓
Event Router (watch + resync)
     ↓
Ingester (enrichment)
     ↓
Worker Queue
     ↓
Batch Writer
     ↓
PostgreSQL
```

### Key Components

**Event Router**

- Watches namespaces
- Periodically resyncs
- Applies throttling to prevent event storms

**Ingester**

- Extracts useful metadata
- Resolves object relationships
- Generates globally unique identifiers
- Serializes events as JSON

**Queue + Workers**

- Buffers ingestion jobs
- Enables concurrent processing

**Batch Writer**

- Groups inserts for better database performance  

**Cache Cleaner**

- Periodically clears internal resolver caches.


## Features

- Kubernetes-native (uses `InClusterConfig`)
- High-throughput batching
- Structured logging
- Automatic cluster name detection
- Event enrichment pipeline
- Backpressure-safe queue
- Health probes
- Graceful shutdown
- Cloud-ready design

## Stored Event Model

Each Kubernetes event is transformed into a structured record containing:

- Cluster name
- Namespace
- Resource kind and name
- Event type and reason
- Human-readable message
- Composition ID (when available)
- Creation timestamp
- Resource version
- Raw JSON payload
- Globally unique ID (`cluster:eventUID`)

Events are **minified** before storage to reduce payload size.


## Requirements

- Kubernetes cluster
- PostgreSQL
- Network connectivity between the service and the database
- RBAC permissions to watch events


## Configuration

The application is configured via environment variables.

| Variable | Description | Default |
|------------|----------------|------------|
| `PORT` | Health probe server port | `8080` (implementation-dependent) |
| `DB_USER` | Database username | — |
| `DB_PASS` | Database password | — |
| `DB_NAME` | Database name | — |
| `DB_HOST` | Database host | — |
| `DB_PORT` | Database port | `5432` |
| `DB_PARAMS` | Extra connection parameters | — |
| `DB_READY_TIMEOUT` | Max wait for PostgreSQL readiness | `2m` |
| `NAMESPACES` | Namespaces to watch | all (if empty) |
| `OTEL_ENABLED` | Enable OpenTelemetry metrics export | `false` |
| `OTEL_EXPORT_INTERVAL` | OpenTelemetry metric export interval | `50s` |

> The service builds the PostgreSQL connection string from these values.


## How It Works

### 1. Startup Sequence

- Waits for PostgreSQL to become available
- Detects the cluster name
- Starts the health probe server
- Initializes the Kubernetes client
- Launches workers and batch processor
- Begins watching events


### Event Flow

1. Kubernetes emits an event.
2. The router forwards it to the ingester.
3. The ingester:

   - Resolves related objects  
   - Extracts metadata  
   - Attaches composition ID  
   - Builds a structured record  

4. The record becomes a queued job.
5. The batch worker writes it to PostgreSQL.

## Health Probes

The service exposes probe endpoints suitable for Kubernetes deployments.

| Endpoint | Purpose |
|------------|------------|
| `/livez` | Process is alive |
| `/readyz` | Database reachable and service ready |



## Deployment Notes

Recommended for Kubernetes deployment.

Best practices:

- Store DB credentials in Secrets
- Use resource limits
- Monitor queue depth
- Enable PostgreSQL connection pooling
- Co-locate with the database when possible to reduce latency

## Telemetry

This project includes ready-to-use telemetry assets under [`telemetry`](telemetry):

- OpenTelemetry Collector sample config
- Grafana dashboard
- Metrics reference with PromQL examples
