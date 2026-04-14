using System.Text.Json;
using System.Threading.Channels;
using JobSvc.Data;
using JobSvc.Models;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace JobSvc.Consumers;

public sealed partial class JobStatusBroadcastConsumer : BackgroundService
{
    private readonly IRabbitMqConnectionManager _connectionManager;
    private readonly ChannelWriter<StatusUpdate> _writer;
    private readonly ILogger<JobStatusBroadcastConsumer> _logger;

    private static readonly JsonSerializerOptions DeserializeOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public JobStatusBroadcastConsumer(
        IRabbitMqConnectionManager connectionManager,
        ChannelWriter<StatusUpdate> writer,
        ILogger<JobStatusBroadcastConsumer> logger)
    {
        _connectionManager = connectionManager;
        _writer = writer;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var connection = await _connectionManager.GetConnectionAsync(stoppingToken);
        using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

        // Exclusive auto-delete queue — one per instance, destroyed when connection closes
        var queueDeclare = await channel.QueueDeclareAsync(
            queue: "",
            durable: false,
            exclusive: true,
            autoDelete: true,
            arguments: null,
            cancellationToken: stoppingToken);

        await channel.QueueBindAsync(
            queue: queueDeclare.QueueName,
            exchange: RabbitMqInitializer.BroadcastExchangeName,
            routingKey: "",
            cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, ea) =>
        {
            try
            {
                var message = JsonSerializer.Deserialize<PrintStatusMessage>(ea.Body.Span, DeserializeOptions);
                if (message is null) return Task.CompletedTask;

                var update = new StatusUpdate(
                    message.JobId.ToString(),
                    message.Status.ToLower(),
                    message.Printed,
                    message.Total,
                    message.Error);

                _writer.TryWrite(update);
                LogUpdateReceived(_logger, update.JobId, update.Status);
            }
            catch (Exception ex)
            {
                LogDeserializationFailed(_logger, ex);
            }

            return Task.CompletedTask;
        };

        await channel.BasicConsumeAsync(
            queue: queueDeclare.QueueName,
            autoAck: true,
            consumer: consumer,
            cancellationToken: stoppingToken);

        LogStarted(_logger);
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "JobStatusBroadcastConsumer started.")]
    private static partial void LogStarted(ILogger logger);

    [LoggerMessage(Level = LogLevel.Information, Message = "Broadcast update received for job {JobId}: {Status}.")]
    private static partial void LogUpdateReceived(ILogger logger, string jobId, string status);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to deserialize broadcast message.")]
    private static partial void LogDeserializationFailed(ILogger logger, Exception ex);
}
