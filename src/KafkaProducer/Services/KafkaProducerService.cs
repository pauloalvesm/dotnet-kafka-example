using Confluent.Kafka;
using KafkaProducer.Models;
using System.Text.Json;

namespace KafkaProducer.Services;

public class KafkaProducerService : IDisposable
{
    private readonly IProducer<Null, string> _producer;

    public KafkaProducerService(string bootstrapServers)
        : this(new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
    {
    }

    public KafkaProducerService(IProducer<Null, string> producer)
    {
        _producer = producer ?? throw new ArgumentNullException(nameof(producer));
    }

    public async Task<bool> PublishTransactionAsync(string topic, TransactionEvent transaction)
    {
        string jsonPayload = JsonSerializer.Serialize(transaction);

        try
        {
            var message = new Message<Null, string> { Value = jsonPayload };
            var result = await _producer.ProduceAsync(topic, message);

            return result.Status == PersistenceStatus.Persisted;
        }
        catch (ProduceException<Null, string> e)
        {
            Console.WriteLine($"[Kafka Error] Failed to deliver message: {e.Error.Reason}");
            return false;
        }
    }

    public void Dispose()
    {
        _producer?.Flush(TimeSpan.FromSeconds(10));
        _producer?.Dispose();
    }
}
