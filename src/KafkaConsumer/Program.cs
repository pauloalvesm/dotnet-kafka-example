using Confluent.Kafka;
using KafkaConsumer.Models;
using System.Text.Json;

internal class Program
{
    private static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "transaction-processor-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        string topic = "transaction-topic";
        consumer.Subscribe(topic);

        using var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (_, e) => {
            e.Cancel = true;
            cts.Cancel();
        };

        Console.WriteLine($"--- Transaction Consumer Started ({topic}) ---\n");
        Console.WriteLine("Waiting for new messages... (Press Ctrl+C to exit)\n");

        try
        {
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(cts.Token);
                    var jsonPayload = result.Message.Value;

                    var transaction = JsonSerializer.Deserialize<TransactionEvent>(jsonPayload);

                    if (transaction != null)
                    {
                        Console.WriteLine($"[New Transaction Received]:");
                        Console.WriteLine($"  ID: {transaction.Id}");
                        Console.WriteLine($"  Store: {transaction.StoreName}");
                        Console.WriteLine($"  Amount: ${transaction.Amount:N2}");
                        Console.WriteLine($"  Date/Time: {transaction.CreatedAt.ToLocalTime()}");
                        Console.WriteLine(new string('-', 40));
                    }
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"[Error consuming message]: {e.Error.Reason}");
                }
                catch (JsonException)
                {
                    Console.WriteLine("[Error] Failed to deserialize the received JSON payload.");
                }
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("\nProcessing stopped successfully.");
        }
        finally
        {
            consumer.Close();
        }
    }
}