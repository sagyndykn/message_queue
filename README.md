# Message Queue

A lightweight HTTP message queue built with **Spring Boot** and **MongoDB**.

Producers publish messages to named topics; consumers pull them back in **FIFO**
order. Every message is delivered **exactly once**, even under heavy concurrent
consumption.

---

## Features

- **Topics** with a configurable capacity.
- **Produce** — publish a message to a topic.
- **Consume** — pop the oldest message from a topic.
- **FIFO ordering** — messages come out in the order they went in.
- **Exactly-once delivery** — no duplicates and no lost messages under concurrency.
- **Backpressure** — producing to a full queue is rejected instead of overflowing.

---

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/topics` | Create a topic |
| `GET` | `/topics` | List all topics |
| `GET` | `/topics/{name}` | Get one topic's config |
| `POST` | `/queue` | Produce a message |
| `GET` | `/queue?topic={name}` | Consume the oldest message |

---

## Running locally

Prerequisites: Java 17 and Docker.

```
docker compose up -d      # start MongoDB
./mvnw spring-boot:run    # start the app on http://localhost:8080
```

---

## Reliability under load

The queue is verified with production-style load tests that use it the way a real
broker is used — producers and consumers work the **same topic at the same time**,
not one after the other.

- **Single topic, mixed load:** 8 producers and 12 consumers hit one topic
  simultaneously with 4000 messages.
- **Many topics in parallel:** 5 topics, each with concurrent producers and
  consumers running at once.

Under this concurrency the tests confirm the guarantees a broker must hold:

- **No message loss** — every produced message is eventually delivered.
- **Exactly-once delivery** — no message is ever delivered twice.
- **Topic isolation** — messages never leak between topics.
- **No server errors** — no request fails under load.

---

## Performance metrics

Indicative single-machine numbers (full HTTP round trip). Figures vary with
hardware and load.

| Operation | Mode | Throughput (msg/s) | avg (ms) | p95 (ms) | p99 (ms) |
|---|---|---|---|---|---|
| Produce | sequential | ~90–140 | 7–12 | 13–20 | 20–31 |
| Consume | 10 threads | ~390–750 | 12–24 | 20–48 | 28–102 |