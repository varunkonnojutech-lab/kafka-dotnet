using System.Text.Json;
using Confluent.Kafka;

namespace KafkaDemo.Core;

/// <summary>
/// JSON (de)serializer for Confluent.Kafka. Kafka only moves bytes, so every
/// producer/consumer pair needs one of these. System.Text.Json keeps the
/// payload human-readable in tools like kafka-console-consumer, which makes
/// this repo much easier to debug than an Avro/Protobuf setup.
/// </summary>
public sealed class KafkaJsonSerializer<T> : ISerializer<T>, IDeserializer<T>
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
    };

    public byte[] Serialize(T data, SerializationContext context) =>
        JsonSerializer.SerializeToUtf8Bytes(data, Options);

    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
        JsonSerializer.Deserialize<T>(data, Options)
        ?? throw new InvalidOperationException($"Could not deserialize {typeof(T).Name} from Kafka payload.");
}
