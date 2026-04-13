using System.Text.Json;
using JobSvc.Data;
using JobSvc.Models;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace JobSvc.Consumers;

public sealed partial class PrintStatusConsumer : BackgroundService
{
    private readonly IRabbitMqConnectionManager _connectionManager;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<PrintStatusConsumer> _logger;

    private static readonly JsonSerializerOptions DeserializeOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private static readonly JsonSerializerOptions SerializeOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    public PrintStatusConsumer(
        IRabbitMqConnectionManager connectionManager,
        IServiceScopeFactory scopeFactory,
        ILogger<PrintStatusConsumer> logger)
    {
        _connectionManager = connectionManager;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var connection = await _connectionManager.GetConnectionAsync(stoppingToken);
        using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false, cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            try
            {
                var message = JsonSerializer.Deserialize<PrintStatusMessage>(ea.Body.Span, DeserializeOptions);
                if (message is null)
                {
                    LogInvalidMessage(_logger, ea.DeliveryTag);
                    await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false, cancellationToken: CancellationToken.None);
                    return;
                }

                await HandleMessageAsync(message, stoppingToken);
                await channel.BasicAckAsync(ea.DeliveryTag, multiple: false, cancellationToken: CancellationToken.None);
                LogMessageProcessed(_logger, message.JobId);
            }
            catch (Exception ex)
            {
                LogProcessingFailed(_logger, ex);
                try
                {
                    await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false, cancellationToken: CancellationToken.None);
                }
                catch (Exception nackEx)
                {
                    LogNackFailed(_logger, nackEx);
                }
            }
        };

        await channel.BasicConsumeAsync(
            queue: RabbitMqInitializer.PrintStatusQueue,
            autoAck: false,
            consumerTag: "",
            noLocal: false,
            exclusive: false,
            arguments: null,
            consumer: consumer,
            cancellationToken: stoppingToken);

        LogConsumerStarted(_logger);

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    public async Task HandleMessageAsync(PrintStatusMessage message, CancellationToken ct)
    {
        if (!Enum.TryParse<JobStatus>(message.Status, ignoreCase: true, out var status))
            throw new ArgumentException($"Unknown job status: '{message.Status}'.", nameof(message));

        await using var scope = _scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<JobDbContext>();

        if (db.Database.IsRelational())
        {
            await using var tx = await db.Database.BeginTransactionAsync(ct);
            await UpdateJobAsync(db, message, status, ct);
            var payload = JsonSerializer.Serialize(message, SerializeOptions);
            await db.Database.ExecuteSqlAsync($"SELECT pg_notify('job_status', {payload})", ct);
            await tx.CommitAsync(ct);
        }
        else
        {
            await UpdateJobAsync(db, message, status, ct);
        }
    }

    private static async Task UpdateJobAsync(JobDbContext db, PrintStatusMessage message, JobStatus status, CancellationToken ct)
    {
        var job = await db.Jobs.FindAsync([message.JobId], ct)
            ?? throw new InvalidOperationException($"Job {message.JobId} not found.");

        job.Status = status;
        job.Printed = message.Printed;
        job.Error = message.Error;
        job.UpdatedAt = DateTimeOffset.UtcNow;

        await db.SaveChangesAsync(ct);
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "PrintStatusConsumer started, listening to 'print.status'.")]
    private static partial void LogConsumerStarted(ILogger logger);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Received null or invalid print.status message at delivery tag {DeliveryTag}. Discarding.")]
    private static partial void LogInvalidMessage(ILogger logger, ulong deliveryTag);

    [LoggerMessage(Level = LogLevel.Information, Message = "Job {JobId} status updated successfully from print.status.")]
    private static partial void LogMessageProcessed(ILogger logger, Guid jobId);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to process print.status message. Discarding (no requeue).")]
    private static partial void LogProcessingFailed(ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to send NACK after processing error.")]
    private static partial void LogNackFailed(ILogger logger, Exception ex);
}
