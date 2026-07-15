using Confluent.Kafka;
using KafkaConsumer.Models;
using KafkaConsumer.Services;
using Moq;
using System.Text.Json;

namespace KafkaProducer.Test.Services;

public class KafkaConsumerServiceTest
{
    private readonly Mock<IConsumer<Ignore, string>> _consumerMock;
    private readonly string _testTopic = "transaction-topic";
    private readonly KafkaConsumerService _sut;

    public KafkaConsumerServiceTest()
    {
        _consumerMock = new Mock<IConsumer<Ignore, string>>();
        _sut = new KafkaConsumerService(_consumerMock.Object, _testTopic);
    }

    [Fact(DisplayName = "StartConsuming should invoke callback and successfully process message when valid JSON payload is received")]
    public void StartConsuming_WhenMessageIsValidJson_ShouldInvokeCallback()
    {
        // Arrange
        var transaction = new TransactionEvent
        {
            Id = Guid.NewGuid(),
            StoreName = "Test Store",
            Amount = 150.00m,
            CreatedAt = DateTime.UtcNow
        };
        string jsonPayload = JsonSerializer.Serialize(transaction);

        var consumeResult = new ConsumeResult<Ignore, string>
        {
            Message = new Message<Ignore, string> { Value = jsonPayload }
        };

        using var cts = new CancellationTokenSource();

        _consumerMock
            .Setup(c => c.Consume(It.IsAny<CancellationToken>()))
            .Callback(() => cts.Cancel())
            .Returns(consumeResult);

        TransactionEvent receivedTransaction = null;
        Action<TransactionEvent> callback = (tx) => receivedTransaction = tx;

        // Act
        _sut.StartConsuming(callback, cts.Token);

        // Assert
        _consumerMock.Verify(c => c.Subscribe(_testTopic), Times.Once);
        _consumerMock.Verify(c => c.Close(), Times.Once);
        Assert.NotNull(receivedTransaction);
        Assert.Equal(transaction.Id, receivedTransaction.Id);
        Assert.Equal("Test Store", receivedTransaction.StoreName);
    }

    [Fact(DisplayName = "StartConsuming should handle JsonException and continue loop when payload is structurally invalid")]
    public void StartConsuming_WhenMessageIsInvalidJson_ShouldCatchSerializationErrorAndContinue()
    {
        // Arrange
        string invalidJson = "{ invalid json data }";
        var consumeResult = new ConsumeResult<Ignore, string>
        {
            Message = new Message<Ignore, string> { Value = invalidJson }
        };

        using var cts = new CancellationTokenSource();

        _consumerMock
            .Setup(c => c.Consume(It.IsAny<CancellationToken>()))
            .Callback(() => cts.Cancel())
            .Returns(consumeResult);

        bool callbackInvoked = false;
        Action<TransactionEvent> callback = (_) => callbackInvoked = true;

        // Act
        _sut.StartConsuming(callback, cts.Token);

        // Assert
        _consumerMock.Verify(c => c.Subscribe(_testTopic), Times.Once);
        _consumerMock.Verify(c => c.Close(), Times.Once);
        Assert.False(callbackInvoked);
    }

    [Fact(DisplayName = "StartConsuming should catch and log ConsumeException when a native Kafka consumer error occurs")]
    public void StartConsuming_WhenConsumeThrowsConsumeException_ShouldHandleExceptionAndContinue()
    {
        // Arrange
        var error = new Error(ErrorCode.Local_ValueSerialization, "Value deserialization issue");

        var exceptionToThrow = new ConsumeException((ConsumeResult<byte[], byte[]>)null!, error);

        using var cts = new CancellationTokenSource();

        _consumerMock
            .Setup(c => c.Consume(It.IsAny<CancellationToken>()))
            .Callback(() => cts.Cancel())
            .Throws(exceptionToThrow);

        bool callbackInvoked = false;
        Action<TransactionEvent> callback = (_) => callbackInvoked = true;

        // Act
        _sut.StartConsuming(callback, cts.Token);

        // Assert
        _consumerMock.Verify(c => c.Subscribe(_testTopic), Times.Once);
        _consumerMock.Verify(c => c.Close(), Times.Once);
        Assert.False(callbackInvoked);
    }

    [Fact(DisplayName = "StartConsuming should stop processing and trigger OperationCanceledException when token is canceled")]
    public void StartConsuming_WhenTokenCanceledBeforeConsume_ShouldStopGracefully()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        bool callbackInvoked = false;
        Action<TransactionEvent> callback = (_) => callbackInvoked = true;

        // Act
        _sut.StartConsuming(callback, cts.Token);

        // Assert
        _consumerMock.Verify(c => c.Subscribe(_testTopic), Times.Once);
        _consumerMock.Verify(c => c.Consume(It.IsAny<CancellationToken>()), Times.Never);
        _consumerMock.Verify(c => c.Close(), Times.Once);
        Assert.False(callbackInvoked);
    }

    [Fact(DisplayName = "Dispose should properly release and dispose internal Kafka consumer resources")]
    public void Dispose_WhenCalled_ShouldDisposeInternalConsumer()
    {
        // Arrange & Act
        _sut.Dispose();

        // Assert
        _consumerMock.Verify(c => c.Dispose(), Times.Once);
    }
}
