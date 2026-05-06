# Traceflow - An Observability Platform

A distributed log ingestion and querying system built using Go, Clickhouse, Redis, and Docker, demonstrating real-world
observability backend patterns used in platforms

**No frontend.** curl / Postman is the client. The focus is on the backend pipeline.

---

## Architecture

```
External Client (curl / Postman)
        │
        │  HTTP POST /ingest  (JSON log event)
        ▼
┌─────────────────────────┐
│   Ingestion Service      │   :8080
│   (Go + Fiber)           │
│                          │
│  1. Validate input       │
│  2. Attach trace_id      │
│  3. LPUSH → Redis queue  │
│  4. gRPC call → Worker   │──────────────────┐
└─────────────────────────┘                   │ gRPC :50051
                                              ▼
                                   ┌───────────────────────┐
                                   │    Worker Service      │
                                   │    (Go)                │
                                   │                        │
                                   │  - gRPC server         │
                                   │  - BRPOP Redis loop    │
                                   │  - Batch 20 logs       │
                                   │  - Write → ClickHouse  │
                                   └───────────────────────┘
                                               │
                                               ▼
                                        ┌────────────┐
                                        │ ClickHouse │  :9000
                                        │  (logs DB) │
                                        └────────────┘
                                               ▲
        │  HTTP GET /logs                      │
        ▼                                      │
┌─────────────────────────┐                    │
│   Query Service          │   :8081           │
│   (Go + Fiber)           │───────────────────┘
│                          │  SELECT from ClickHouse
└─────────────────────────┘
```

---

## Tech Stack

| Layer           | Technology                          | Notes                            |
|-----------------|-------------------------------------|----------------------------------|
| Language        | Go 1.22+                            |                                  |
| HTTP Framework  | go-fiber/fiber                      | Fast, Express-like               |
| gRPC            | google.golang.org/grpc              | Internal service comms           |
| Protobuf        | google.golang.org/protobuf          | Schema & code generation         |
| ClickHouse      | clickhouse-go                       | Columnar log storage             |
| Redis           | redis                               | Ingestion queue and cache        |
| Trace ID        | crypto/rand                         | Lightweight trace propagation    |
| Config          | joho/godotenv                       | .env loading                     |
| Logging         | log/slog                            | Structured logging (Go 1.21+)    |
| Containerisation| Docker + docker-compose             | Full local stack                 |

---

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Go 1.26+

### Start all services

```bash
# Clone the repository
git clone https://github.com/sahilverma/observability-platform
cd observability-platform

# Copy the example env file
cp .env.example .env

# Start everything (ClickHouse, Redis, all 3 Go services)
cd deployments
docker-compose up --build
```

On first startup, ClickHouse automatically runs `migrations/001_create_logs.sql`
to create the `observability` database and `logs` table.

### Ingest a log event

```bash
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "service": "auth-service",
    "level": "ERROR",
    "message": "DB connection failed",
    "metadata": {"user_id": "42", "host": "db-primary"}
  }'

# Response:
# {"status":"accepted","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"}
```

The response is `202 Accepted` (not 201 Created) because the log is queued for async processing,
not yet written to ClickHouse.

### Query logs

```bash
# All logs
curl "http://localhost:8081/logs"

# Filter by service and level
curl "http://localhost:8081/logs?service=auth-service&level=ERROR"

# Time range filter
curl "http://localhost:8081/logs?from=2026-05-05T00:00:00Z&to=2026-05-05T23:59:59Z"

# Paginate
curl "http://localhost:8081/logs?limit=50&offset=0"

# Response:
# {"count":3,"logs":[{"Timestamp":"2026-05-05T10:00:00Z","Service":"auth-service",...}]}
```

### Health checks

```bash
curl http://localhost:8080/health   # Ingestion service
curl http://localhost:8081/health   # Query service
```

---

## Design Decisions

### Why ClickHouse over PostgreSQL?

ClickHouse is a **columnar store**
Log data is written once and read many times with aggregations (`GROUP BY level`,
time-range scans). PostgreSQL is a row store — every query reads all columns of every
matching row, even if you only need `level` and `message`.

ClickHouse reads only the columns you `SELECT`. For wide log tables with millions of rows

Additionally, ClickHouse's `MergeTree` engine is designed for bulk inserts

### Why Redis queue between Ingestion and Worker?

Without a queue, slow ClickHouse writes would block every `POST /ingest` HTTP response —
the client would wait seconds for a single log to be confirmed written to disk.

With Redis:
- **Ingestion latency** = Redis LPUSH latency
- **Storage latency** = async, handled by the Worker in batches
- **Spike tolerance**: a traffic burst fills Redis (fast) and the Worker drains at a steady pace
- **Batching**: the Worker writes 20+ logs per ClickHouse INSERT — far more efficient than
  20 individual inserts (each of which creates a separate disk part)

### Why gRPC for Ingestion → Worker signal?

The gRPC call is an **optimisation signal** — it tells the Worker to drain the queue immediately
rather than waiting for the 5-second polling interval. This reduces log-to-ClickHouse latency.

gRPC is used (instead of HTTP) because:
- **Typed contract**: the `.proto` file defines the interface; a schema mismatch is a compile error
- **Binary protocol**: protobuf is faster to encode/decode than JSON for high-frequency internal calls
- **Production mirroring**: real observability pipelines use gRPC for internal communications

If the gRPC call fails, the Worker's polling loop still picks up the logs within 5 seconds.
The Redis queue is the source of truth, not the gRPC signal.

### Why a custom trace_id instead of OpenTelemetry?

OTel is the industry standard, but it makes this project more complex.

`internal/traceid` does this in 15 lines using only stdlib. The trace_id format (32-char hex)

---

## Project Structure

```
observability-platform/
├── cmd/
│   ├── ingestion/main.go     # Ingestion Service entry point
│   ├── worker/main.go        # Worker Service entry point
│   └── query/main.go         # Query Service entry point
├── internal/
│   ├── config/config.go      # Typed config from env vars
│   ├── traceid/traceid.go    # Lightweight trace ID generation
│   ├── ingestion/            # HTTP handler, service, validator, model
│   ├── worker/               # gRPC server, consumer loop, batch processor
│   ├── query/                # HTTP handler, service, repository
│   ├── grpc/
│   │   ├── proto/log.proto   # Protobuf schema
│   │   ├── gen/              # Generated Go code (log.pb.go, log_grpc.pb.go)
│   │   └── client.go         # gRPC client for Ingestion → Worker
│   ├── queue/                # Redis producer and consumer wrappers
│   └── clickhouse/           # ClickHouse client and log repository
├── pkg/middleware/
│   └── request_id.go         # Fiber middleware: trace ID propagation
├── migrations/
│   └── 001_create_logs.sql   # ClickHouse DDL
├── deployments/
│   ├── docker-compose.yml
│   └── Dockerfile            # Multi-stage, single file for all 3 services
├── .env.example
└── README.md
```

---

## Development — Regenerating Protobuf

If you modify `internal/grpc/proto/log.proto`, regenerate the Go code:

```bash
# Install tools (one-time setup)
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# macOS
brew install protobuf

# Ubuntu
apt install -y protobuf-compiler

# Regenerate (run from project root)
protoc --go_out=./internal/grpc/gen --go_opt=paths=source_relative \
       --go-grpc_out=./internal/grpc/gen --go-grpc_opt=paths=source_relative \
       internal/grpc/proto/log.proto
```

---

## Development — Running locally without Docker

```bash
# Start infrastructure only
cd deployments
docker-compose up clickhouse redis

# In separate terminals:
cd ../..
go run ./cmd/worker
go run ./cmd/ingestion
go run ./cmd/query
```

Make sure `.env` exists (copy from `.env.example`) with `localhost` addresses.

---

## Inspecting the data

```bash
# Connect to ClickHouse
docker exec -it deployments-clickhouse-1 clickhouse-client --database=observability

# Query logs
SELECT * FROM logs ORDER BY timestamp DESC LIMIT 10;
SELECT level, count() FROM logs GROUP BY level;

# Connect to Redis
docker exec -it deployments-redis-1 redis-cli
LLEN logs_queue      # queue depth
LRANGE logs_queue 0 4  # peek at first 5 items
```

## This project is under construction
- the README.md file though describes every aspect of the project, even those that are not working yet