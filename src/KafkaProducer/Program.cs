using Confluent.Kafka;
using KafkaProducer.Models;
using System.Text.Json;

internal class Program
{
    private static async Task Main(string[] args)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();
        string topic = "transaction-topic";

        Console.WriteLine($"--- Transaction Producer Started ({topic}) ---");
        Console.WriteLine("Press Enter to send a new simulated transaction (or type 'exit' to quit):\n");

        while (true)
        {
            string? command = Console.ReadLine();
            if (command?.ToLower() == "exit") break;

            var newTransaction = new TransactionEvent
            {
                Id = Guid.NewGuid(),
                StoreName = "Central Tech Store",
                Amount = new Random().Next(50, 1500),
                CreatedAt = DateTime.UtcNow
            };

            string jsonPayload = JsonSerializer.Serialize(newTransaction);

            try
            {
                var result = await producer.ProduceAsync(topic, new Message<Null, string> { Value = jsonPayload });

                Console.WriteLine($"[Success] Transaction {newTransaction.Id} sent! Amount: ${newTransaction.Amount:N2}");
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"[Error] Failed to send message: {e.Error.Reason}");
            }
        }
    }
}