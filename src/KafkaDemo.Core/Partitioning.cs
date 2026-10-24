namespace KafkaDemo.Core;

/// <summary>
/// Why keys matter, in one class.
///
/// Kafka guarantees ordering <em>within a partition</em>, not across a topic.
/// Give every message for the same customer the same key and Kafka's
/// default partitioner routes them all to one partition, so ordering holds
/// per customer even as you scale consumers.
///
/// This class re-implements that idea with a stable FNV-1a hash so the
/// behaviour is unit-testable without a broker. The real producer in
/// KafkaDemo.Producer relies on Kafka's built-in keyed partitioner, which
/// follows the same rule (same key -&gt; same partition); this class exists to
/// prove the rule, not to replace the partitioner.
/// </summary>
public static class Partitioning
{
    /// <summary>The Kafka message key for an order event: the customer id.</summary>
    public static string KeyFor(OrderEvent order) => order.CustomerId;

    /// <summary>
    /// Deterministically maps a key to a partition in [0, partitionCount).
    /// Same key always yields the same partition; different keys spread out.
    /// </summary>
    public static int PartitionFor(string key, int partitionCount)
    {
        if (partitionCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(partitionCount));
        if (string.IsNullOrEmpty(key))
            throw new ArgumentException("Key must not be empty.", nameof(key));

        // FNV-1a 32-bit: stable across runs and processes, unlike GetHashCode().
        uint hash = 2166136261;
        foreach (char c in key)
        {
            hash ^= c;
            hash *= 16777619;
        }
        return (int)(hash % (uint)partitionCount);
    }
}
