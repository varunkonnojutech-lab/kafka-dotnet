// KafkaDemo.Consumer — reads the "orders" topic as part of a consumer group.
//
// Run:  dotnet run --project src/KafkaDemo.Consumer
// Env:  KAFKA_BOOTSTRAP_SERVERS (default localhost:9092)
//
// Requires a running broker: `docker compose up -d` in the repo root.
// Run the producer first, then this consumer; press Ctrl+C to stop.
//
// The three consumer decisions that matter most, all demonstrated here:
//   1. Manual commits (EnableAutoCommit = false). The offset is committed
//      only AFTER the message is handled, so a crash replays the message
//      instead of losing it. (At-least-once delivery: handlers must be
//      idempotent — in real systems, dedupe on OrderId.)
//   2. Consumer groups. Start two instances of this app and Kafka splits
//      the partitions between them; kill one and Kafka rebalances.
//   3. Dead letters. A message that fails validation is poison — retrying
//      cannot fix it — so it goes to "orders.dlq" and the consumer moves
//      on instead of blocking the partition forever. Transient failures
//      retry with exponential backoff (see KafkaDemo.Core.RetryPolicy).

using Confluent.Kafka;
using KafkaDemo.Core;

string bootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
var retry = new RetryPolicy();

var consumerConfig = new ConsumerConfig
{
    BootstrapServers = bootstrap,
    GroupId = "kafka-demo-group",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
};

var producerConfig = new ProducerConfig
{
    BootstrapServers = bootstrap,
    Acks = Acks.All,
    EnableIdempotence = true,
};

using var consumer = new ConsumerBuilder<string, OrderEvent>(consumerConfig)
    .SetValueDeserializer(new KafkaJsonSerializer<OrderEvent>())
    .Build();
using var dlqProducer = new ProducerBuilder<string, OrderEvent>(producerConfig)
    .SetValueSerializer(new KafkaJsonSerializer<OrderEvent>())
    .Build();

consumer.Subscribe(OrderEvent.Topics.Orders);
Console.WriteLine($"Subscribed to '{OrderEvent.Topics.Orders}' (group kafka-demo-group). Press Ctrl+C to stop.");

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

// Stand-in for a downstream call (payment service, database, ...).
// Orders over $350 "fail transiently" so the retry + DLQ path is visible
// in the demo; everything else succeeds. Replace with your real handler.
static void Process(OrderEvent order)
{
    if (order.Amount > 350)
        throw new InvalidOperationException("downstream service temporarily unavailable");
}
try
{
    while (!cts.IsCancellationRequested)
    {
        ConsumeResult<string, OrderEvent>? result;
        try
        {
            result = consumer.Consume(cts.Token);
        }
        catch (OperationCanceledException)
        {
            break;
        }

        var order = result.Message.Value;

        // Poison first: an invalid payload can never succeed, so it goes
        // straight to the DLQ with no retries (see RetryPolicy.Decide).
        if (!order.IsValid(out string reason))
        {
            Console.WriteLine($"[poison] order={order.OrderId} ({reason}) -> DLQ");
            await dlqProducer.ProduceAsync(
                new TopicPartition(OrderEvent.Topics.OrdersDeadLetter, Partition.Any),
                new Message<string, OrderEvent> { Key = order.CustomerId, Value = order });
            consumer.Commit(result);
            continue;
        }

        // In-memory retry loop: process, and on a transient failure wait
        // with exponential backoff and try again. The offset is committed
        // only after success (or after the DLQ write), so a crash mid-way
        // replays the message instead of losing it.
        bool handled = false;
        for (int attempt = 1; attempt <= retry.MaxAttempts && !handled; attempt++)
        {
            try
            {
                Process(order); // the "real work" — replace with your handler
                handled = true;
            }
            catch (InvalidOperationException ex) when (attempt < retry.MaxAttempts)
            {
                var delay = retry.DelayBeforeAttempt(attempt);
                Console.WriteLine($"[retry] order={order.OrderId} attempt {attempt} failed ({ex.Message}); waiting {delay.TotalSeconds}s");
                await Task.Delay(delay, cts.Token);
            }
        }

        if (handled)
        {
            Console.WriteLine($"[handled] order={order.OrderId} customer={order.CustomerId} " +
                              $"amount={order.Amount} partition={result.Partition.Value} offset={result.Offset.Value}");
        }
        else
        {
            Console.WriteLine($"[dlq] order={order.OrderId} failed {retry.MaxAttempts} attempts -> DLQ");
            await dlqProducer.ProduceAsync(
                new TopicPartition(OrderEvent.Topics.OrdersDeadLetter, Partition.Any),
                new Message<string, OrderEvent> { Key = order.CustomerId, Value = order });
        }
        consumer.Commit(result);
    }
}
finally
{
    consumer.Close();
    Console.WriteLine("Consumer stopped.");
}
