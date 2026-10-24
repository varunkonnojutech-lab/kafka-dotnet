namespace KafkaDemo.Core;

/// <summary>
/// Retry and dead-letter rules for the consumer, kept broker-free so they
/// can be unit tested.
///
/// The shape that survives production: exponential backoff with a cap
/// (so a struggling downstream doesn't get hammered), plus a hard stop —
/// after <see cref="MaxAttempts"/> the message goes to the dead-letter
/// topic instead of blocking the partition forever. A poison message that
/// fails validation never retries at all: retrying cannot fix it.
/// </summary>
public sealed class RetryPolicy
{
    public int MaxAttempts { get; init; } = 3;
    public TimeSpan BaseDelay { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Delay before attempt N (1-based). 1s, 2s, 4s, ... capped at MaxDelay.</summary>
    public TimeSpan DelayBeforeAttempt(int attempt)
    {
        if (attempt < 1)
            throw new ArgumentOutOfRangeException(nameof(attempt));
        double seconds = BaseDelay.TotalSeconds * Math.Pow(2, attempt - 1);
        return TimeSpan.FromSeconds(Math.Min(seconds, MaxDelay.TotalSeconds));
    }

    /// <summary>
    /// Decides what the consumer does with a failed message.
    /// Returns "retry" (with the delay), "dead-letter", or "poison"
    /// (invalid payload — straight to the DLQ, no retries).
    /// </summary>
    public (string Action, TimeSpan Delay) Decide(OrderEvent order, int attemptsSoFar)
    {
        if (!order.IsValid(out _))
            return ("poison", TimeSpan.Zero);
        if (attemptsSoFar >= MaxAttempts)
            return ("dead-letter", TimeSpan.Zero);
        return ("retry", DelayBeforeAttempt(attemptsSoFar + 1));
    }
}
