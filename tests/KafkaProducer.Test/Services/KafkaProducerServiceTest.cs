using Confluent.Kafka;
using KafkaProducer.Models;
using KafkaProducer.Services;
using Moq;

namespace KafkaProducer.Test.Services;

public class KafkaProducerServiceTest
{
    private readonly Mock<IProducer<Null, string>> _producerMock;
    private readonly KafkaProducerService _sut;

    public KafkaProducerServiceTest()
    {
        _producerMock = new Mock<IProducer<Null, string>>();
        _sut = new KafkaProducerService(_producerMock.Object);
    }

    [Fact(DisplayName = "PublishTransactionAsync should return true when the Kafka broker successfully persists the message")]
    public async Task PublishTransactionAsync_WhenMessageIsPersisted_ShouldReturnTrue()
    {
        // Arrange
        string topic = "test-topic";
        var transaction = new TransactionEvent
        {
            Id = Guid.NewGuid(),
            StoreName = "Test Store",
            Amount = 100.00m,
            CreatedAt = DateTime.UtcNow
        };

        var expectedResult = new DeliveryResult<Null, string>
        {
            Status = PersistenceStatus.Persisted
        };

        _producerMock
            .Setup(p => p.ProduceAsync(topic, It.IsAny<Message<Null, string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedResult);

        // Act
        bool result = await _sut.PublishTransactionAsync(topic, transaction);

        // Assert
        Assert.True(result);
        _producerMock.Verify(p => p.ProduceAsync(topic, It.Is<Message<Null, string>>(m => m.Value.Contains("Test Store")), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact(DisplayName = "PublishTransactionAsync should return false when the Kafka broker acknowledges but fails to persist the message")]
    public async Task PublishTransactionAsync_WhenMessageIsNotPersisted_ShouldReturnFalse()
    {
        // Arrange
        string topic = "test-topic";
        var transaction = new TransactionEvent { Id = Guid.NewGuid(), StoreName = "Test Store" };

        var expectedResult = new DeliveryResult<Null, string>
        {
            Status = PersistenceStatus.NotPersisted
        };

        _producerMock
            .Setup(p => p.ProduceAsync(topic, It.IsAny<Message<Null, string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedResult);

        // Act
        bool result = await _sut.PublishTransactionAsync(topic, transaction);

        // Assert
        Assert.False(result);
    }

    [Fact(DisplayName = "PublishTransactionAsync should catch ProduceException and return false when a network or broker infrastructure failure occurs")]
    public async Task PublishTransactionAsync_WhenProduceThrowsProduceException_ShouldCatchAndReturnFalse()
    {
        // Arrange
        string topic = "test-topic";
        var transaction = new TransactionEvent { Id = Guid.NewGuid(), StoreName = "Test Store" };

        var kafkaError = new Error(ErrorCode.Local_Transport, "Connection refused by local broker");
        var deliveryResult = new DeliveryResult<Null, string> { Status = PersistenceStatus.NotPersisted };
        var exceptionToThrow = new ProduceException<Null, string>(kafkaError, deliveryResult);

        _producerMock
            .Setup(p => p.ProduceAsync(topic, It.IsAny<Message<Null, string>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(exceptionToThrow);

        // Act
        bool result = await _sut.PublishTransactionAsync(topic, transaction);

        // Assert
        Assert.False(result);
    }

    [Fact(DisplayName = "Dispose should flush any pending messages and properly release internal Kafka producer resources")]
    public void Dispose_WhenCalled_ShouldFlushAndDisposeInternalProducer()
    {
        // Arrange & Act
        _sut.Dispose();

        // Assert
        _producerMock.Verify(p => p.Flush(It.IsAny<TimeSpan>()), Times.Once);
        _producerMock.Verify(p => p.Dispose(), Times.Once);
    }
}