using JobSvc.Data;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;

namespace JobSvc.Tests.Data;

public class RabbitMqConnectionManagerTests
{
    private readonly Mock<IConnectionFactory> _factoryMock;
    private readonly Mock<IOptions<RabbitMqOptions>> _optionsMock;
    private readonly Mock<ILogger<RabbitMqConnectionManager>> _loggerMock;
    private readonly RabbitMqOptions _options;

    public RabbitMqConnectionManagerTests()
    {
        _factoryMock = new Mock<IConnectionFactory>();
        _optionsMock = new Mock<IOptions<RabbitMqOptions>>();
        _loggerMock = new Mock<ILogger<RabbitMqConnectionManager>>();
        _options = new RabbitMqOptions
        {
            Uri = "amqp://guest:guest@localhost:5672/"
        };
        _optionsMock.Setup(o => o.Value).Returns(_options);
    }

    [Fact]
    public async Task GetConnectionAsync_Returns_Connection_On_Success()
    {
        // Arrange
        var connectionMock = new Mock<IConnection>();
        _factoryMock.Setup(f => f.CreateConnectionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(connectionMock.Object);

        var manager = new RabbitMqConnectionManager(_factoryMock.Object, _optionsMock.Object, _loggerMock.Object);

        // Act
        var result = await manager.GetConnectionAsync();

        // Assert
        Assert.Same(connectionMock.Object, result);
        _factoryMock.Verify(f => f.CreateConnectionAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact] 
    public async Task GetConnectionAsync_Should_Retry_On_Failure()
    {
        // Arrange
        var connectionMock = new Mock<IConnection>();
        int calls = 0;
        _factoryMock.Setup(f => f.CreateConnectionAsync(It.IsAny<CancellationToken>()))
            .Returns(async () => {
                calls++;
                if (calls == 1) throw new Exception("First attempt fails");
                return await Task.FromResult(connectionMock.Object);
            });

        var manager = new RabbitMqConnectionManager(_factoryMock.Object, _optionsMock.Object, _loggerMock.Object);

        // Act
        // We use a short timeout/cancellation to avoid infinite loop if it fails, 
        // but since we return success on 2nd attempt it should be fine.
        var result = await manager.GetConnectionAsync();

        // Assert
        Assert.Same(connectionMock.Object, result);
        Assert.Equal(2, calls);
    }

    [Fact]
    public async Task GetConnectionAsync_Returns_Same_Connection_Instance_When_Open()
    {
        // Arrange
        var connectionMock = new Mock<IConnection>();
        connectionMock.Setup(c => c.IsOpen).Returns(true);
        _factoryMock.Setup(f => f.CreateConnectionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(connectionMock.Object);

        var manager = new RabbitMqConnectionManager(_factoryMock.Object, _optionsMock.Object, _loggerMock.Object);

        // Act
        var conn1 = await manager.GetConnectionAsync();
        var conn2 = await manager.GetConnectionAsync();

        // Assert
        Assert.Same(conn1, conn2);
        _factoryMock.Verify(f => f.CreateConnectionAsync(It.IsAny<CancellationToken>()), Times.Once);
    }
}
