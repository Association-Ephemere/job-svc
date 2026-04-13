using System.Text.Json;
using System.Threading.Channels;
using JobSvc.Data;
using JobSvc.Models;
using Npgsql;

namespace JobSvc.Listeners;

public sealed partial class JobStatusListener : BackgroundService
{
    private readonly INpgsqlConnectionFactory _connFactory;
    private readonly ChannelWriter<StatusUpdate> _writer;
    private readonly ILogger<JobStatusListener> _logger;

    private static readonly JsonSerializerOptions DeserializeOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public JobStatusListener(
        INpgsqlConnectionFactory connFactory,
        ChannelWriter<StatusUpdate> writer,
        ILogger<JobStatusListener> logger)
    {
        _connFactory = connFactory;
        _writer = writer;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var delay = TimeSpan.FromSeconds(1);
        const int maxDelaySeconds = 30;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using var conn = _connFactory.Create();
                await conn.OpenAsync(stoppingToken);

                conn.Notification += (_, args) => HandleNotification(args.Payload);

                await using var cmd = new NpgsqlCommand("LISTEN job_status", conn);
                await cmd.ExecuteNonQueryAsync(stoppingToken);

                delay = TimeSpan.FromSeconds(1); // reset backoff after successful connect
                LogListening(_logger);

                while (!stoppingToken.IsCancellationRequested)
                    await conn.WaitAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                LogConnectionLost(_logger, ex, delay.TotalSeconds);
                try
                {
                    await Task.Delay(delay, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                delay = TimeSpan.FromSeconds(Math.Min(delay.TotalSeconds * 2, maxDelaySeconds));
            }
        }
    }

    public void HandleNotification(string payload)
    {
        try
        {
            var update = JsonSerializer.Deserialize<StatusUpdate>(payload, DeserializeOptions);
            if (update is null)
            {
                LogInvalidPayload(_logger, payload);
                return;
            }

            _writer.TryWrite(update);
            LogNotificationReceived(_logger, update.JobId, update.Status);
        }
        catch (Exception ex)
        {
            LogDeserializationFailed(_logger, ex, payload);
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "JobStatusListener connected, listening to 'job_status'.")]
    private static partial void LogListening(ILogger logger);

    [LoggerMessage(Level = LogLevel.Warning, Message = "JobStatusListener connection lost. Reconnecting in {Delay}s...")]
    private static partial void LogConnectionLost(ILogger logger, Exception ex, double delay);

    [LoggerMessage(Level = LogLevel.Information, Message = "Notification received for job {JobId}: {Status}.")]
    private static partial void LogNotificationReceived(ILogger logger, string jobId, string status);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Received null payload from job_status notification: '{Payload}'.")]
    private static partial void LogInvalidPayload(ILogger logger, string payload);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to deserialize job_status notification payload: '{Payload}'.")]
    private static partial void LogDeserializationFailed(ILogger logger, Exception ex, string payload);
}
