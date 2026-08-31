# worker-queue

A job queue broker in Go, in the shape of RabbitMQ or SQS. Producers post tasks
over HTTP; workers long-poll for them, do the work, and report back. The broker
tracks what's in flight, retries failures with exponential backoff, and sets
aside tasks that never succeed.

Built with the standard library only — no third-party dependencies.

```
producer  ──POST /jobs──►  broker  ──GET /jobs/next──►  worker
                             │      ◄──ack / nack────      │
                        holds, tracks,                appends to
                        retries, buries                 jobs.log
```

## Quick start

Three terminals.

```bash
go run ./cmd/broker
```

```bash
go run ./cmd/worker -concurrency 3
```

```bash
go run ./cmd/producer -n 100
```

Then check that all 100 landed and the queue drained:

```bash
wc -l jobs.log && curl -s localhost:8080/stats
```

## How it works

The broker keeps every task in exactly one of four places. All the interesting
behaviour is a move between two of them.

| Collection | Holds |
|---|---|
| `ready` | waiting to be picked up, oldest first |
| `handedOut` | out with a worker, not yet confirmed |
| `delayed` | failed, waiting out a backoff before the next attempt |
| `dead` | out of attempts — the dead-letter queue |

```
POST /jobs ──► ready ──GET /jobs/next──► handedOut ──ack──► done
                 ▲                          │
                 │                          ├──nack───────────┐
                 │                          └──hold expires───┤
              (sweeper                                        ▼
               promotes)                            attempts left?
               delayed ◄────────── yes ─────────────────┤
                                                        └── no ──► dead
```

### Leases

Every handout carries a random `lease_id` and a deadline. Acks and nacks must
present the matching lease id, so a worker whose deadline already passed cannot
confirm a task that has since been given to somebody else — it gets a `409`
instead.

This is the same idea as RabbitMQ's delivery tag, and it's what keeps the
broker's bookkeeping correct when a slow worker comes back from the dead.

### Attempts

`attempts` counts **deliveries, not failures**, and increments when a task is
handed out. A worker that crashes mid-task still burns an attempt, so a task
that kills workers can't loop forever.

### Retries

A failed task waits before its next attempt: roughly 1s, 2s, 4s, 8s… capped at
30s. Each delay is half fixed and half random, so a batch of tasks that failed
together don't all retry at the same instant and knock over whatever they were
failing against.

Once `attempts` reaches `max_attempts`, the task goes to the dead-letter queue
with its last error, where you can inspect it and requeue it by hand.

### The sweeper

One goroutine, ticking every 250ms. It does the two things nothing else can,
because both are driven by the clock rather than by a request:

- **Reclaims tasks from workers that went silent.** A crashed worker can't nack
  — that's what crashed means. Without this, its task would sit in `handedOut`
  forever: not ready, not delayed, not dead. Just stuck.
- **Returns delayed tasks when their wait is over**, and wakes any worker
  parked in a long-poll so the retry happens promptly instead of whenever
  someone next happens to ask.

### Long-polling

`GET /jobs/next` holds the connection open until a task turns up, the wait
elapses, or the worker disconnects. Idle workers cost nothing — they're parked
on a channel, using no CPU, until an enqueue wakes them.

## HTTP API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/jobs` | enqueue a task |
| `GET` | `/jobs/next` | claim the oldest ready task (long-polls) |
| `POST` | `/jobs/{id}/ack` | report success |
| `POST` | `/jobs/{id}/nack` | report failure |
| `GET` | `/stats` | current counts and lifetime totals |
| `GET` | `/dlq` | list tasks that ran out of attempts |
| `POST` | `/dlq/{id}/requeue` | give a dead task a clean start |
| `GET` | `/healthz` | liveness |

### `POST /jobs`

```json
{
  "payload": {"line": "hello"},
  "max_attempts": 3,
  "delay_ms": 0
}
```

`payload` is required and may be any JSON. `max_attempts` defaults to 3.
`delay_ms` holds the task back before it can be picked up.

→ `201 Created` with `{"id": "a3f9..."}`

### `GET /jobs/next?wait_ms=20000&lease_ms=30000`

`wait_ms` is how long to hold the connection open (default 20s, capped at 30s).
`lease_ms` is how long the worker is claiming the task for (default 30s, capped
at 5 minutes).

→ `200 OK`

```json
{
  "task": {
    "id": "a3f9...",
    "payload": {"line": "hello"},
    "attempts": 1,
    "max_attempts": 3,
    "created_at": "...",
    "available_at": "..."
  },
  "lease_id": "7c21...",
  "deadline": "..."
}
```

→ `204 No Content` when the wait elapsed with nothing available. **This is the
normal idle answer, not an error** — the worker simply asks again.

### `POST /jobs/{id}/ack?lease_id=...`

→ `204` on success · `404` if not held by anyone · `409` if the lease id doesn't
match

### `POST /jobs/{id}/nack?lease_id=...`

Optional body: `{"error": "what went wrong"}`

→ `204` · `404` · `409` as above

### `GET /stats`

```json
{
  "ready": 0,
  "handed_out": 0,
  "delayed": 0,
  "dead": 5,
  "totals": {
    "enqueued": 15,
    "delivered": 25,
    "acked": 10,
    "nacked": 15,
    "reclaimed": 0,
    "retried": 10,
    "buried": 5,
    "requeued": 0
  }
}
```

The top-level fields are **gauges** — how many tasks are in each collection
right now. `totals` are **counters** — what has happened since the broker
started. Gauges tell you the queue is empty; counters tell you whether that's
because everything succeeded or everything died.

One invariant worth knowing: every failure is either reported or noticed, and
leads to either a retry or a burial, so

```
nacked + reclaimed  ==  retried + buried
```

If those two sides ever disagree, a task has leaked out of `handedOut` by a path
that isn't accounted for.

### `POST /dlq/{id}/requeue`

Resets `attempts` to 0, clears the last error, and puts the task back in `ready`
for a full fresh set of attempts.

→ `204` · `404` if the id isn't in the dead-letter queue

## Commands

### `cmd/broker`

| Flag | Default | Meaning |
|---|---|---|
| `-addr` | `:8080` | address to listen on |

### `cmd/worker`

Claims tasks and appends each one to a log file as a JSON line.

| Flag | Default | Meaning |
|---|---|---|
| `-broker` | `http://localhost:8080` | broker address |
| `-log-file` | `./jobs.log` | file to append to |
| `-concurrency` | `3` | how many workers to run |
| `-wait-ms` | `20000` | how long to wait for a task |
| `-lease-ms` | `30000` | how long to claim a task for |

A payload containing `{"fail": true}` makes the worker fail on purpose. That's
the hook for exercising retries and the dead-letter queue.

### `cmd/producer`

A test harness standing in for whatever would really create work.

| Flag | Default | Meaning |
|---|---|---|
| `-broker` | `http://localhost:8080` | broker address |
| `-n` | `1` | how many tasks to enqueue |
| `-fail` | `false` | enqueue tasks that always fail |
| `-max-attempts` | `3` | attempts before a task is buried |
| `-delay-ms` | `0` | hold each task back before it can be picked up |
| `-concurrency` | `1` | how many tasks to post at once |

## Things to try

**The happy path** — 100 lines in `jobs.log`, all gauges back to zero:

```bash
go run ./cmd/producer -n 100
```

**Retries and the dead-letter queue** — three attempts each, ~1s then ~2s
apart, then buried:

```bash
go run ./cmd/producer -n 5 -fail -max-attempts 3
```

```bash
curl -s localhost:8080/dlq
```

**Delayed tasks** — `stats` shows `delayed: 10` for five seconds, then drains:

```bash
go run ./cmd/producer -n 10 -delay-ms 5000
```

**Contention** — 50 producers against 3 workers, the first real stress on the
store's lock:

```bash
go run ./cmd/producer -n 2000 -concurrency 50
```

**Crash recovery** — claim a task by hand with a one-second lease and never
confirm it. Within a couple of seconds the sweeper reclaims it, `reclaimed`
increments, and the task returns to `ready`:

```bash
curl -s "localhost:8080/jobs/next?wait_ms=2000&lease_ms=1000"
```

```bash
curl -s localhost:8080/stats
```

## Layout

```
cmd/
  broker/       HTTP server; owns the store and starts the sweeper
  worker/       claims tasks, appends them to a log file
  producer/     CLI for enqueuing test work
internal/
  task/         the Task model and id generation
  store/
    store.go    the Store interface, Delivery, errors, retry policy
    memory/     in-memory implementation + the sweeper
  broker/       HTTP routing, handlers, request/response types
  worker/       broker client, worker loop, log sink
  metrics/      lifetime counters
```

`internal/store` holds the contract; implementations live in subpackages beside
`memory`. Adding a durable backend means a new folder implementing the same
methods and one line changed in `cmd/broker` — nothing above the store moves.

The broker declares the interface it needs (`broker.TaskQueue`) rather than the
store guessing at it, which is the usual Go convention and keeps handlers
testable against a fake.

## What this does and doesn't promise

Given a `201` from `POST /jobs`, the broker guarantees:

- the task will be attempted **at least once**
- failures are retried up to `max_attempts`, with a growing gap between tries
- if a worker dies holding it, it will be handed to somebody else
- if it exhausts every attempt it is **kept** in the DLQ, not silently dropped
- tasks are delivered roughly oldest-first

It explicitly does **not** promise:

- **Exactly-once delivery.** A slow worker is indistinguishable from a dead one,
  so a task can genuinely run twice. Task handlers must tolerate that —
  appending a line to a file is fine; charging a customer would not be.
- **Durability.** The in-memory store loses everything on restart. This is the
  backend's limitation, not the design's, and is the main reason the store sits
  behind an interface.
- **Ordering guarantees.** Retries and delays reorder tasks by design.

## Status

Feature-complete against the v1 plan. Known gaps:

- **No tests.** This is the significant one — there is real concurrency in three
  packages with nothing guarding it.
- No persistence, no named queues or topics, no priorities, no auth or TLS.
- `delayed` and the lease scan are linear; both would want a heap if either list
  ever got large.
