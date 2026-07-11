using Confluent.Kafka;
using KafkaConsumer.Models;
using KafkaConsumer.Services;
using System.Text.Json;

internal class Program
{
    static void Main(string[] args)
    {
        string bootstrapServers = "localhost:9092";
        string groupId = "transaction-processor-group";
        string topic = "transaction-topic";

        using var consumerService = new KafkaConsumerService(bootstrapServers, groupId, topic);
        using var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (_, e) => {
            e.Cancel = true;
            cts.Cancel();
        };

        Console.WriteLine($"--- Transaction Consumer Started ({topic}) ---");
        Console.WriteLine("Waiting for transactions... Press Ctrl+C to stop.\n");

        consumerService.StartConsuming(ProcessTransaction, cts.Token);
    }

    private static void ProcessTransaction(TransactionEvent transaction)
    {
        Console.WriteLine($"[Processing Transaction]:");
        Console.WriteLine($"  ID: {transaction.Id}");
        Console.WriteLine($"  Store: {transaction.StoreName}");
        Console.WriteLine($"  Amount: ${transaction.Amount:N2}");
        Console.WriteLine($"  Date/Time: {transaction.CreatedAt.ToLocalTime()}");
        Console.WriteLine(new string('-', 40));
    }
}