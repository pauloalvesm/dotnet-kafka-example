using Confluent.Kafka;
using KafkaConsumer.Models;
using System.Text.Json;

namespace KafkaConsumer.Services;

public class KafkaConsumerService : IDisposable
{
    private readonly IConsumer<Ignore, string> _consumer;
    private readonly string _topic;

    public KafkaConsumerService(string bootstrapServers, string groupId, string topic)
            : this(new ConsumerBuilder<Ignore, string>(new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true
            }).Build(), topic)
    {
    }

    public KafkaConsumerService(IConsumer<Ignore, string> consumer, string topic)
    {
        _consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
        _topic = topic ?? throw new ArgumentNullException(nameof(topic));
    }

    public void StartConsuming(Action<TransactionEvent> onMessageReceived, CancellationToken cancellationToken)
    {
        _consumer.Subscribe(_topic);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var result = _consumer.Consume(cancellationToken);
                    string jsonPayload = result.Message.Value;

                    var transaction = JsonSerializer.Deserialize<TransactionEvent>(jsonPayload);

                    if (transaction != null)
                    {
                        onMessageReceived(transaction);
                    }
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"[Kafka Consumer Error]: {e.Error.Reason}");
                }
                catch (JsonException)
                {
                    Console.WriteLine("[Serialization Error] Failed to parse JSON payload.");
                }
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("[Kafka] Consumer loop canceled gracefully.");
        }
        finally
        {
            _consumer.Close();
        }
    }

    public void Dispose()
    {
        _consumer?.Dispose();
    }
}
