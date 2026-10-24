# Kafka in .NET — producer, consumer, partitions, retries & dead letters

[![Build](https://github.com/varunkonnojutech-lab/kafka-dotnet/actions/workflows/ci.yml/badge.svg)](https://github.com/varunkonnojutech-lab/kafka-dotnet/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![.NET 8](https://img.shields.io/badge/.NET-8.0-512BD4)](https://dotnet.microsoft.com/)
[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/varunkonnojutech-lab/kafka-dotnet)

Kafka shows up on nearly every senior .NET job description — and most tutorials stop at "produce a string, consume a string." This repo goes one step further and shows the parts that actually matter in production: **keyed partitioning for ordering, exactly-once-ish producer settings, manual commits, exponential-backoff retries, and a dead-letter topic for poison messages.**

## Who is this for

.NET developers who have heard of Kafka (or used RabbitMQ) and want a runnable, honest picture of how a Kafka-based service really behaves — including what happens when a message is bad and what happens when a downstream service is down.

## What you'll learn

- **Producer done right** — `Acks.All` + idempotence so an acknowledged write survives a broker crash and a retried produce never duplicates an event
- **Partitioning & ordering** — Kafka orders *within a partition*, not across a topic; keying by `CustomerId` keeps one customer's orders in order while the topic scales
- **Consumer groups & rebalancing** — run two consumers and watch Kafka split the partitions; kill one and watch it rebalance
- **At-least-once delivery** — manual commits *after* handling, so a crash replays instead of losing a message (and why handlers must be idempotent — dedupe on `OrderId`)
- **Retries with backoff** — exponential delays with a cap, so a struggling downstream isn't hammered
- **Dead letters** — poison messages (invalid payloads) skip retries entirely; repeatedly-failing messages land in `orders.dlq` instead of blocking the partition forever

## Quick start

You need Docker (for Kafka) and .NET 8. The unit tests run with no broker at all:

```bash
dotnet test
```

For the live demo, start a single-node Kafka (KRaft mode, no ZooKeeper):

```bash
docker compose up -d
```

Then, in two terminals:

```bash
# Terminal 1 — the consumer (leave it running)
dotnet run --project src/KafkaDemo.Consumer

# Terminal 2 — produce 50 order events
dotnet run --project src/KafkaDemo.Producer -- --count 50
```

Watch for three things in the consumer output:

1. **Same customer, same partition** — all events for `cust-101` arrive on one partition, in order.
2. **A poison message** — the last produced event has a negative amount, so the consumer routes it to `orders.dlq` instead of retrying.
3. **A retried message** — orders over $350 "fail transiently" (a stand-in for a downed downstream service), so you see `[retry]` lines with growing delays before the message lands in the DLQ.

Peek at the raw topics any time with:

```bash
docker exec kafka-demo-broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders.dlq --from-beginning
```

Stop Kafka when you're done: `docker compose down`.

## The mental model, in one table

| Concept | What it means here |
|---|---|
| Topic (`orders`) | The log every event is appended to |
| Partition | An ordered sub-log; ordering holds *inside* one, not across the topic |
| Key (`CustomerId`) | Decides the partition — same key, same partition, same order |
| Consumer group (`kafka-demo-group`) | Consumers in one group split the partitions; add consumers to scale |
| Offset + manual commit | "I've handled up to here" — committed *after* handling, never before |
| DLQ (`orders.dlq`) | Where messages go when retrying can never help |

## The delivery-semantics trap

Kafka gives you **at-least-once** delivery with this setup, not exactly-once: if the consumer crashes after handling a message but before committing, that message is delivered again. The fix is idempotency on the consumer side — in a real system, check `OrderId` against a processed-events store before applying side effects. Exactly-once *transactions* exist in Kafka but cost throughput and complexity; most teams choose at-least-once + idempotent handlers, which is what this repo demonstrates.

## Project layout

```
src/KafkaDemo.Core/       OrderEvent, JSON (de)serializer, partitioning,
                          retry + dead-letter rules (all broker-free, unit tested)
src/KafkaDemo.Producer/   Console producer: Acks.All, idempotence, keyed sends
src/KafkaDemo.Consumer/   Console consumer: manual commits, retries, DLQ
tests/KafkaDemo.Tests/    Serializer round-trips, partition stability,
                          backoff math, poison vs retry decisions
docker-compose.yml        Single-node Kafka (KRaft) for the live demo
```

## Next steps

- Swap the JSON serializer for Avro + Schema Registry when a schema contract matters.
- Replace the in-memory retry loop with a retry topic + delayed redelivery so a slow message doesn't hold up its partition.
- Try it for real: point `KAFKA_BOOTSTRAP_SERVERS` at a Confluent Cloud cluster and run the producer unchanged.
