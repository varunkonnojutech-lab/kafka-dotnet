namespace KafkaDemo.Core;

/// <summary>
/// The event that flows through Kafka. One record = one order placed.
///
/// The <see cref="CustomerId"/> doubles as the Kafka message key, which is
/// the whole point of the demo: every event for the same customer lands in
/// the same partition, so a consumer always sees that customer's orders in
/// the order they were placed.
/// </summary>
public sealed record OrderEvent
{
    public required Guid OrderId { get; init; }
    public required string CustomerId { get; init; }
    public required decimal Amount { get; init; }
    public required string Currency { get; init; }
    public required int ItemCount { get; init; }
    public required DateTimeOffset CreatedAtUtc { get; init; }

    /// <summary>Topic names used by the producer and consumer in this repo.</summary>
    public static class Topics
    {
        public const string Orders = "orders";
        public const string OrdersDeadLetter = "orders.dlq";
    }

    /// <summary>
    /// Business validation, shared by the producer (fail fast) and the
    /// consumer (poison-message detection). An event that fails this check
    /// can never succeed by retrying, so the consumer routes it to the
    /// dead-letter topic instead of retrying forever.
    /// </summary>
    public bool IsValid(out string reason)
    {
        if (string.IsNullOrWhiteSpace(CustomerId)) { reason = "CustomerId is empty."; return false; }
        if (Amount <= 0) { reason = "Amount must be greater than zero."; return false; }
        if (ItemCount <= 0) { reason = "ItemCount must be greater than zero."; return false; }
        if (string.IsNullOrWhiteSpace(Currency)) { reason = "Currency is empty."; return false; }
        reason = string.Empty;
        return true;
    }
}
