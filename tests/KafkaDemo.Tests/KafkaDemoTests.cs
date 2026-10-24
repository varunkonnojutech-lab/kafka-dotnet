using Confluent.Kafka;
using KafkaDemo.Core;
using Xunit;

namespace KafkaDemo.Tests;

public class OrderEventTests
{
    private static OrderEvent ValidOrder() => new()
    {
        OrderId = Guid.NewGuid(),
        CustomerId = "cust-101",
        Amount = 99.95m,
        Currency = "USD",
        ItemCount = 2,
        CreatedAtUtc = DateTimeOffset.UtcNow,
    };

    [Fact]
    public void ValidOrder_PassesValidation()
    {
        Assert.True(ValidOrder().IsValid(out string reason));
        Assert.Equal(string.Empty, reason);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void NonPositiveAmount_IsInvalid(decimal amount)
    {
        var order = ValidOrder() with { Amount = amount };
        Assert.False(order.IsValid(out _));
    }

    [Fact]
    public void EmptyCustomer_IsInvalid()
    {
        var order = ValidOrder() with { CustomerId = "" };
        Assert.False(order.IsValid(out _));
    }
}

public class SerializerTests
{
    [Fact]
    public void RoundTrip_PreservesEveryField()
    {
        var serializer = new KafkaJsonSerializer<OrderEvent>();
        var original = new OrderEvent
        {
            OrderId = Guid.Parse("11111111-2222-3333-4444-555555555555"),
            CustomerId = "cust-202",
            Amount = 42.50m,
            Currency = "EUR",
            ItemCount = 3,
            CreatedAtUtc = new DateTimeOffset(2026, 10, 24, 12, 0, 0, TimeSpan.Zero),
        };

        byte[] bytes = serializer.Serialize(original, new SerializationContext(MessageComponentType.Value, "orders"));
        var restored = serializer.Deserialize(bytes, false, new SerializationContext(MessageComponentType.Value, "orders"));

        Assert.Equal(original, restored);
    }

    [Fact]
    public void Payload_IsReadableJson()
    {
        var serializer = new KafkaJsonSerializer<OrderEvent>();
        var order = new OrderEvent
        {
            OrderId = Guid.NewGuid(), CustomerId = "cust-1", Amount = 10m,
            Currency = "USD", ItemCount = 1, CreatedAtUtc = DateTimeOffset.UtcNow,
        };
        string json = System.Text.Encoding.UTF8.GetString(
            serializer.Serialize(order, new SerializationContext(MessageComponentType.Value, "orders")));
        Assert.Contains("cust-1", json);
        Assert.Contains("customerId", json); // camelCase naming policy
    }
}

public class PartitioningTests
{
    [Fact]
    public void SameKey_AlwaysMapsToSamePartition()
    {
        int first = Partitioning.PartitionFor("cust-101", 6);
        for (int i = 0; i < 20; i++)
            Assert.Equal(first, Partitioning.PartitionFor("cust-101", 6));
    }

    [Fact]
    public void Partition_IsAlwaysInRange()
    {
        foreach (string key in new[] { "a", "cust-101", "customer-with-a-very-long-id-12345", "x" })
        {
            int p = Partitioning.PartitionFor(key, 12);
            Assert.InRange(p, 0, 11);
        }
    }

    [Fact]
    public void DifferentKeys_SpreadAcrossPartitions()
    {
        // 100 distinct customers over 6 partitions should touch them all.
        var seen = new HashSet<int>();
        for (int i = 0; i < 100; i++)
            seen.Add(Partitioning.PartitionFor($"cust-{i}", 6));
        Assert.Equal(6, seen.Count);
    }

    [Fact]
    public void KeyFor_UsesCustomerId()
    {
        var order = new OrderEvent
        {
            OrderId = Guid.NewGuid(), CustomerId = "cust-303", Amount = 5m,
            Currency = "USD", ItemCount = 1, CreatedAtUtc = DateTimeOffset.UtcNow,
        };
        Assert.Equal("cust-303", Partitioning.KeyFor(order));
    }
}

public class RetryPolicyTests
{
    private static OrderEvent ValidOrder() => new()
    {
        OrderId = Guid.NewGuid(), CustomerId = "cust-1", Amount = 10m,
        Currency = "USD", ItemCount = 1, CreatedAtUtc = DateTimeOffset.UtcNow,
    };

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 4)]
    public void Backoff_DoublesEachAttempt(int attempt, double expectedSeconds)
    {
        var policy = new RetryPolicy();
        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), policy.DelayBeforeAttempt(attempt));
    }

    [Fact]
    public void Backoff_IsCappedAtMaxDelay()
    {
        var policy = new RetryPolicy { MaxDelay = TimeSpan.FromSeconds(5) };
        Assert.Equal(TimeSpan.FromSeconds(5), policy.DelayBeforeAttempt(10));
    }

    [Fact]
    public void InvalidOrder_GoesStraightToPoison()
    {
        var policy = new RetryPolicy();
        var poison = ValidOrder() with { Amount = -1 };
        var (action, _) = policy.Decide(poison, 0);
        Assert.Equal("poison", action);
    }

    [Fact]
    public void ValidOrder_RetriesUntilMaxAttempts_ThenDeadLetters()
    {
        var policy = new RetryPolicy { MaxAttempts = 3 };
        var order = ValidOrder();
        Assert.Equal("retry", policy.Decide(order, 0).Action);
        Assert.Equal("retry", policy.Decide(order, 2).Action);
        Assert.Equal("dead-letter", policy.Decide(order, 3).Action);
    }
}
