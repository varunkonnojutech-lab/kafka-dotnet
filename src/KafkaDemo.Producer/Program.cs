// KafkaDemo.Producer — publishes order events to the "orders" topic.
//
// Run:  dotnet run --project src/KafkaDemo.Producer [--count 50]
// Env:  KAFKA_BOOTSTRAP_SERVERS (default localhost:9092)
//
// Requires a running broker: `docker compose up -d` in the repo root
// starts a single-node Kafka (KRaft, no ZooKeeper) on localhost:9092.
//
// The three producer settings that matter most, all enabled here:
//   Acks.All          — the leader waits for all in-sync replicas, so an
//                       acknowledged write survives a broker crash.
//   EnableIdempotence — the broker deduplicates retries, so a retried
//                       produce never creates a duplicate event.
//   Key = CustomerId  — same customer -> same partition -> per-customer
//                       ordering is preserved (see KafkaDemo.Core.Partitioning).

using Confluent.Kafka;
using KafkaDemo.Core;

int count = 50;
for (int i = 0; i < args.Length - 1; i++)
    if (args[i] == "--count" && int.TryParse(args[i + 1], out int n) && n > 0)
        count = n;

string bootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";

var config = new ProducerConfig
{
    BootstrapServers = bootstrap,
    Acks = Acks.All,
    EnableIdempotence = true,
    MessageTimeoutMs = 10_000,
};

var customers = new[] { "cust-101", "cust-202", "cust-303", "cust-404", "cust-505" };
var random = new Random(42); // fixed seed: every demo run produces the same story
using var producer = new ProducerBuilder<string, OrderEvent>(config)
    .SetValueSerializer(new KafkaJsonSerializer<OrderEvent>())
    .Build();

int delivered = 0;
for (int i = 0; i < count; i++)
{
    var customer = customers[i % customers.Length];
    var order = new OrderEvent
    {
        OrderId = Guid.NewGuid(),
        CustomerId = customer,
        Amount = Math.Round((decimal)(random.NextDouble() * 400 + 10), 2),
        Currency = "USD",
        ItemCount = random.Next(1, 6),
        CreatedAtUtc = DateTimeOffset.UtcNow,
    };

    // Deliberately poison one message so the consumer demo has something
    // to dead-letter: an order with a negative amount can never be valid.
    if (i == count - 1)
        order = order with { Amount = -1 };

    try
    {
        var result = await producer.ProduceAsync(
            new TopicPartition(OrderEvent.Topics.Orders, Partition.Any),
            new Message<string, OrderEvent> { Key = order.CustomerId, Value = order });
        delivered++;
        Console.WriteLine($"[produced] order={order.OrderId} customer={customer} " +
                          $"partition={result.Partition.Value} offset={result.Offset.Value}");
    }
    catch (ProduceException<string, OrderEvent> ex)
    {
        Console.WriteLine($"[FAILED] order={order.OrderId}: {ex.Error.Reason}");
    }
}

producer.Flush(TimeSpan.FromSeconds(10));
Console.WriteLine($"Done: {delivered}/{count} events delivered to topic '{OrderEvent.Topics.Orders}'.");
