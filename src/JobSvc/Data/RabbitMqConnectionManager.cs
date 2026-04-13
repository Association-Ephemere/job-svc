using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace JobSvc.Data;

public sealed partial class RabbitMqConnectionManager : IRabbitMqConnectionManager
{
    private readonly ConnectionFactory _connectionFactory;
    private readonly ILogger<RabbitMqConnectionManager> _logger;
    private IConnection? _connection;
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private bool _disposed;

    public RabbitMqConnectionManager(
        IOptions<RabbitMqOptions> options,
        ILogger<RabbitMqConnectionManager> logger)
    {
        _logger = logger;
        _connectionFactory = new ConnectionFactory
        {
            Uri = new Uri(options.Value.Uri),
            AutomaticRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5)
        };
    }

    public async Task<IConnection> GetConnectionAsync(CancellationToken ct = default)
    {
        if (_connection is { IsOpen: true })
        {
            return _connection;
        }

        await _semaphore.WaitAsync(ct);
        try
        {
            if (_connection is { IsOpen: true })
            {
                return _connection;
            }

            _connection = await ConnectWithRetryAsync(ct);
            return _connection;
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private async Task<IConnection> ConnectWithRetryAsync(CancellationToken ct)
    {
        var delay = TimeSpan.FromSeconds(1);
        const int maxDelaySeconds = 30;
        int attempt = 1;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                LogConnecting(_logger, attempt);
                var connection = await _connectionFactory.CreateConnectionAsync(ct);
                
                connection.ConnectionShutdownAsync += async (sender, args) =>
                {
                    if (args.Initiator != ShutdownInitiator.Application)
                    {
                        LogConnectionLost(_logger, args.ReplyText);
                    }
                    await Task.CompletedTask;
                };

                // In RabbitMQ.Client 7, these events are also async
                connection.RecoverySucceededAsync += async (sender, args) =>
                {
                    LogConnectionRecovered(_logger);
                    await Task.CompletedTask;
                };
                
                connection.ConnectionRecoveryErrorAsync += async (sender, args) =>
                {
                    LogRecoveryAttemptFailed(_logger, args.Exception);
                    await Task.CompletedTask;
                };

                LogConnectionSuccess(_logger, attempt);
                return connection;
            }
            catch (Exception ex)
            {
                LogConnectionAttemptFailed(_logger, ex, attempt, delay.TotalSeconds);
                
                await Task.Delay(delay, ct);
                
                var nextDelaySeconds = delay.TotalSeconds * 2;
                if (nextDelaySeconds > maxDelaySeconds)
                {
                    nextDelaySeconds = maxDelaySeconds;
                }
                delay = TimeSpan.FromSeconds(nextDelaySeconds);
                attempt++;
            }
        }

        throw new OperationCanceledException(ct);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _connection?.Dispose();
        _semaphore.Dispose();
        _disposed = true;
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Attempting to connect to RabbitMQ (attempt {Attempt})...")]
    private static partial void LogConnecting(ILogger logger, int attempt);

    [LoggerMessage(Level = LogLevel.Information, Message = "Successfully connected to RabbitMQ after {Attempt} attempt(s).")]
    private static partial void LogConnectionSuccess(ILogger logger, int attempt);

    [LoggerMessage(Level = LogLevel.Warning, Message = "RabbitMQ connection attempt {Attempt} failed. Retrying in {Delay}s...")]
    private static partial void LogConnectionAttemptFailed(ILogger logger, Exception ex, int attempt, double delay);

    [LoggerMessage(Level = LogLevel.Warning, Message = "RabbitMQ connection lost. Reason: {Reason}. Automatic recovery in progress...")]
    private static partial void LogConnectionLost(ILogger logger, string reason);

    [LoggerMessage(Level = LogLevel.Information, Message = "RabbitMQ connection recovered successfully.")]
    private static partial void LogConnectionRecovered(ILogger logger);

    [LoggerMessage(Level = LogLevel.Warning, Message = "RabbitMQ automatic recovery attempt failed.")]
    private static partial void LogRecoveryAttemptFailed(ILogger logger, Exception ex);
}
