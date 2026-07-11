using KafkaProducer.Models;
using KafkaProducer.Services;

internal class Program
{
    static async Task Main(string[] args)
    {
        string bootstrapServers = "localhost:9092";
        string topic = "transaction-topic";

        using var kafkaService = new KafkaProducerService(bootstrapServers);

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

            bool isSuccess = await kafkaService.PublishTransactionAsync(topic, newTransaction);

            if (isSuccess)
            {
                Console.WriteLine($"[Success] Transaction {newTransaction.Id} sent! Amount: ${newTransaction.Amount:N2}");
            }
            else
            {
                Console.WriteLine("[Warning] Could not confirm message persistence.");
            }
        }
    }
}