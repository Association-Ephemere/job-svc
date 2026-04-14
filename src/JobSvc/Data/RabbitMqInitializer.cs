using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace JobSvc.Data;

public interface IRabbitMqInitializer
{
    Task InitializeAsync(CancellationToken ct = default);
}

public sealed partial class RabbitMqInitializer : IRabbitMqInitializer
{
    private readonly IRabbitMqConnectionManager _connectionManager;
    private readonly ILogger<RabbitMqInitializer> _logger;

    public const string ExchangeName = "photostand";
    public const string PrintJobsQueue = "print.jobs";
    public const string PrintStatusQueue = "print.status";
    public const string BroadcastExchangeName = "job_status_broadcast";

    public RabbitMqInitializer(
        IRabbitMqConnectionManager connectionManager,
        ILogger<RabbitMqInitializer> logger)
    {
        _connectionManager = connectionManager;
        _logger = logger;
    }

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        _logger.LogInformation("Initializing RabbitMQ infrastructure (exchange, queues, bindings)...");
        
        var connection = await _connectionManager.GetConnectionAsync(ct);
        using var channel = await connection.CreateChannelAsync(cancellationToken: ct);

        var quorumArgs = new Dictionary<string, object?> { ["x-queue-type"] = "quorum" };

        // Declare Exchanges
        await channel.ExchangeDeclareAsync(
            exchange: ExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            cancellationToken: ct);

        await channel.ExchangeDeclareAsync(
            exchange: BroadcastExchangeName,
            type: ExchangeType.Fanout,
            durable: false,
            autoDelete: false,
            cancellationToken: ct);

        // Declare Queues
        await channel.QueueDeclareAsync(
            queue: PrintJobsQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: quorumArgs,
            cancellationToken: ct);

        await channel.QueueDeclareAsync(
            queue: PrintStatusQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: quorumArgs,
            cancellationToken: ct);

        // Bind Queues
        await channel.QueueBindAsync(
            queue: PrintJobsQueue,
            exchange: ExchangeName,
            routingKey: PrintJobsQueue,
            cancellationToken: ct);

        await channel.QueueBindAsync(
            queue: PrintStatusQueue,
            exchange: ExchangeName,
            routingKey: PrintStatusQueue,
            cancellationToken: ct);

        _logger.LogInformation("RabbitMQ infrastructure initialized successfully.");
    }
}
