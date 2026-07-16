# Event Pipeline

An event ingestion and persistence pipeline built as two Go services communicating over Kafka:

```
Client ── HTTP POST /events ──▶ Ingestion ── Kafka (topic: events) ──▶ Persistence ──▶ PostgreSQL
                                                                           │
Client ◀───────────────────────── gRPC (Get / List) ──────────────────────┘
```

1. A client sends an event as JSON to the **ingestion** service (`POST /events`).
2. Ingestion validates the payload, encodes it as protobuf, and publishes it to the `events` Kafka topic, responding with `202 Accepted` — the write is asynchronous.
3. The **persistence** service consumes the topic and synchronously inserts each event into PostgreSQL.
4. Stored events are served back via gRPC (`Get`, `List`).

## Architecture

Both services follow **hexagonal (clean) architecture**. The service layer sits at the center and owns the domain types (`service.Event`), sentinel errors (`service.ErrEventAlreadyExists`, `service.ErrPublishTimeout`, …), and business rules. Everything at the edges — HTTP handlers, gRPC handlers, the Kafka publisher/consumer, the Postgres repository — is an adapter, and **adapters depend on service types, never the other way around**:

- **Driving adapters** (HTTP handler, gRPC handler, Kafka consumer) translate transport-level input into `service.Event` and call the service through a small, locally-defined interface.
- **Driven adapters** (Kafka publisher, Postgres repository) implement interfaces that the *service* defines (`Publisher`, `EventRepository`), translating domain calls into Kafka/SQL and mapping infrastructure errors back into domain sentinels (e.g. a Postgres `23505` unique violation becomes `service.ErrEventAlreadyExists`).

Two Go idioms reinforce the boundary:

- **Interfaces are defined by the consumer**, next to the code that uses them, and kept to 1–3 methods. The handler defines the `EventService` it needs; the service defines the `Publisher`/`EventRepository` it needs. No package imports an adapter to get an interface.
- **Each layer has its own representation of an event** — `handler.Event` (JSON + validation tags), `service.Event` (plain domain struct), `repo.EventModel` (DB mapping), protobuf `Event` — with explicit mapping at each boundary. Transport concerns (JSON tags, SQL column names) never leak into the domain.

The dependency direction is always `cmd → adapters → service`, with `cmd/main.go` acting as the composition root that wires concrete adapters into the service via constructor injection.

### Repository layout

```
ingestion/              Service A
  cmd/                  composition root (config, wiring, lifecycle)
  handler/              HTTP adapter: POST /events, validation, status codes
  service/              domain: Event type, sentinel errors, Publisher port
  publisher/            Kafka adapter: implements service.Publisher
persistence/            Service B
  cmd/                  composition root
  consumer/             Kafka adapter: consumes `events`, drives the service
  rpc/                  gRPC adapter: Get / List endpoints
  service/              domain: Event type, sentinel errors, EventRepository port
  repo/                 Postgres adapter: implements service.EventRepository
    migrations/         SQL migrations (golang-migrate format)
  proto/                gRPC API contract (generated code in gen/)
pkg/
  correlation/          shared correlation-ID context helpers + Kafka header key
  proto/                Kafka wire contract: the protobuf Event message (generated code in gen/)
```

## Service A — Ingestion

- `POST /events` accepts a JSON payload with five fields — `id`, `session_id`, `type`, `message`, `timestamp` (RFC 3339) — all required (validated with `go-playground/validator`). Malformed JSON or a failed validation returns `400 Bad Request`.
- Valid events are published to Kafka **keyed by `session_id`** (hash balancer, `RequiredAcks: all`), and the client receives `202 Accepted`. Keying by session pins all events of one session to one partition, so they preserve their relative order through the pipeline — while the event `id` stays the uniqueness/idempotency key at the database.
- Publish failures are mapped to meaningful responses: a publish timeout returns `503 Service Unavailable`, other failures `500 Internal Server Error`.
- A gRPC server exposes the standard [gRPC health checking protocol](https://grpc.io/docs/guides/health-checking/), used by the container health checks.

## Service B — Persistence

- A Kafka consumer (consumer group `persistence`) reads the `events` topic and writes each event to the `events` table.
- gRPC endpoints:
  - `EventService.Get` — returns a single event by ID (`NOT_FOUND` if absent, `INVALID_ARGUMENT` on empty ID).
  - `EventService.List` — returns all stored events (no pagination, per the spec).
- The same gRPC server also serves the standard health checking protocol.

### Delivery semantics and error handling

Kafka gives **at-least-once** delivery: offsets are committed only *after* an event has been persisted, so a crash between insert and commit causes redelivery rather than data loss. The pipeline is made idempotent one layer down:

- The event ID is the table's **primary key**; the repository maps a unique-violation insert to `service.ErrEventAlreadyExists`.
- The consumer treats that sentinel as success: it logs `duplicate event, skipping` and commits the offset. Effectively exactly-once persistence on top of at-least-once delivery.
- **Poison pills** (messages that fail to unmarshal) can never succeed on retry, so they are logged and committed past instead of blocking the partition.

Store failures are handled by **error classification**: the service layer owns two marker types — `service.RetriableError` and `service.PermanentError` — that adapters use to tell the consumer *why* a store failed, and the consumer turns that into retry policy (classification lives where the knowledge is, the retry loop lives where the consequence is — a blocked partition):

| Error class | Retry | Then |
|---|---|---|
| **Retriable** (e.g. database down) | exponential backoff until success or shutdown | never committed — an outage blocks the partition rather than losing data; shutdown mid-retry leaves the offset uncommitted for redelivery |
| **Permanent** (can never succeed) | none | full event payload is logged at `ERROR` (a lightweight dead-letter record) and the offset is committed, so one hopeless event can't stall the partition |
| **Unclassified** (anything unmarked) | up to `CONSUMER_UNKNOWN_ERROR_RETRY_BUDGET` retries | treated as permanent: logged with payload and committed past |

The unclassified budget is deliberate misclassification insurance: an error nobody anticipated gets the benefit of the doubt (retries, in case it was transient) but only a bounded amount of it (so a wrongly-hopeful classification costs minutes of lag, not a stuck partition). Retry delays start at `CONSUMER_BACKOFF_DURATION`, double per attempt, and are capped at `CONSUMER_MAX_BACKOFF`.

## Kafka wire format

Events travel through Kafka as **protobuf**, not JSON. The message schema lives in [`pkg/proto/event.proto`](pkg/proto/event.proto) — a single contract shared by both sides: the ingestion publisher `proto.Marshal`s it, the persistence consumer `proto.Unmarshal`s it. Compared to the earlier JSON encoding, this replaces two hand-mirrored structs (with "must be kept in sync" comments) by one generated type, so the producer and consumer can no longer drift apart silently — plus a smaller payload and strict typing on the wire.

Deliberately, this is a **separate contract from the gRPC API**: [`persistence/proto/event_service.proto`](persistence/proto/event_service.proto) defines its own `Event` message (proto package `eventservice` vs `event`). The two schemas are identical today, but the queue format and the query API belong to different consumers and evolve on different schedules — the mapping through `service.Event` in each adapter is the seam where they may diverge.

## Correlation IDs

Every request is traceable end-to-end across both services through a correlation ID:

1. The HTTP handler generates a UUID per request and stores it in `context.Context` via the shared `pkg/correlation` package.
2. The Kafka publisher reads it from the context and attaches it as a `correlation_id` **message header** (transport metadata stays out of the payload).
3. The consumer restores the header into its per-message logger, so the persistence path logs under the same ID.

Because the ID travels in the context — Go's carrier for request-scoped metadata, the same mechanism OpenTelemetry uses for trace propagation — no intermediate layer needs to know about it: loggers remain constructor-injected dependencies, and no function signatures change. Both services emit structured JSON logs (`log/slog`), so one grep for a correlation ID reconstructs the full journey:

```
{"level":"DEBUG","msg":"event accepted","component":"handler","correlation_id":"4f3a…","event_id":"evt-1"}
{"level":"DEBUG","msg":"stored event","component":"consumer","correlation_id":"4f3a…","event_id":"evt-1"}
```

Note the correlation ID identifies one *processing pass*, while the event ID identifies the *data* — a redelivered event keeps its event ID but is logged under each delivery's correlation ID.

## Running

Requirements: Docker with Compose.

```sh
docker compose up --build
```

This starts Kafka (KRaft, single broker), PostgreSQL, one-shot init containers (topic creation, database migrations), and both services. The ingestion API listens on `localhost:8080`, the persistence gRPC server on `localhost:9091`.

### Example requests (Bruno)

Example requests for both services ship with the repo as a [Bruno](https://www.usebruno.com/) collection in [`bruno/`](bruno/):

1. Open Bruno → **Open Collection** → select the `bruno/` folder.
2. Pick the **local** environment (top-right dropdown) — it supplies the ingestion URL (`localhost:8080`) and the persistence gRPC address (`localhost:9091`).
3. Run **ingestion / Create Event** to publish an event, then **persistence / Get Event** or **List Events** to query it back over gRPC.

The collection covers the happy path and both validation-failure cases (missing field, malformed JSON) for the HTTP API — each with a status-code assertion — plus the two gRPC queries. gRPC methods resolve via server reflection, or from `persistence/proto/event_service.proto`, which is registered at the collection level.

### Configuration

Both services are configured entirely through environment variables (loaded with viper); all variables are required and validated at startup.

| Service | Variable | Example |
|---|---|---|
| ingestion | `LOG_LEVEL` | `debug` |
| ingestion | `LISTEN_ADDR` | `:8080` |
| ingestion | `GRPC_ADDR` | `:9090` |
| ingestion | `KAFKA_BROKERS` | `kafka:9092` |
| ingestion | `KAFKA_TOPIC` | `events` |
| ingestion | `PUBLISH_TIMEOUT` | `2s` |
| ingestion | `SHUTDOWN_TIMEOUT` | `10s` |
| persistence | `LOG_LEVEL` | `debug` |
| persistence | `GRPC_ADDR` | `:9090` |
| persistence | `KAFKA_BROKERS` | `kafka:9092` |
| persistence | `KAFKA_TOPIC` | `events` |
| persistence | `KAFKA_GROUP_ID` | `persistence` |
| persistence | `DATABASE_URL` | `postgres://…` |
| persistence | `CONSUMER_UNKNOWN_ERROR_RETRY_BUDGET` | `5` |
| persistence | `CONSUMER_BACKOFF_DURATION` | `500ms` |
| persistence | `CONSUMER_MAX_BACKOFF` | `30s` |

Both services shut down gracefully on `SIGINT`/`SIGTERM`: the health endpoint flips to `NOT_SERVING`, in-flight work drains, and the Kafka clients are closed.

## Testing

```sh
go test ./...
```

- **Unit tests** are table-driven and run against mocks generated with [moq](https://github.com/matryer/moq) from the consumer-defined interfaces — a direct payoff of the hexagonal design: every layer is tested in isolation by mocking the interface it consumes.
- **Repository tests** run against a real PostgreSQL instance spun up with [testcontainers](https://golang.testcontainers.org/) and migrated with the same migrations used in deployment (Docker required).
- Adapter error mapping is covered explicitly: e.g. the publisher tests assert that broker timeouts surface as `service.ErrPublishTimeout` and connection failures as `service.ErrBrokerUnavailable`.

## Code generation

```sh
make proto     # regenerate protobuf code — Kafka Event + gRPC stubs (requires protoc + Go plugins)
go generate ./...   # regenerate moq mocks
```
