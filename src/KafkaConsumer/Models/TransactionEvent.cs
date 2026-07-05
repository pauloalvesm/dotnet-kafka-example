namespace KafkaConsumer.Models;

public class TransactionEvent
{
    public Guid Id { get; set; }
    public string StoreName { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public DateTime CreatedAt { get; set; }
}
