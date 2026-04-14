using System.Text.Json;
using JobSvc.Data;
using JobSvc.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace JobSvc.Consumers;

public sealed partial class StuckJobsWatchdog : BackgroundService
{
    private readonly IRabbitMqConnectionManager _connectionManager;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly WatchdogOptions _options;
    private readonly ILogger<StuckJobsWatchdog> _logger;

    private static readonly JsonSerializerOptions SerializeOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    public StuckJobsWatchdog(
        IRabbitMqConnectionManager connectionManager,
        IServiceScopeFactory scopeFactory,
        IOptions<WatchdogOptions> options,
        ILogger<StuckJobsWatchdog> logger)
    {
        _connectionManager = connectionManager;
        _scopeFactory = scopeFactory;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMinutes(_options.IntervalMinutes));

        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                await RunAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                LogRunFailed(_logger, ex);
            }
        }
    }

    public async Task RunAsync(CancellationToken ct)
    {
        var threshold = DateTimeOffset.UtcNow - TimeSpan.FromMinutes(_options.StaleThresholdMinutes);

        IReadOnlyList<Guid> staleJobIds;

        await using (var scope = _scopeFactory.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<JobDbContext>();
            staleJobIds = await db.Jobs
                .Where(j => j.Status == JobStatus.Printing && j.UpdatedAt < threshold)
                .Select(j => j.Id)
                .ToListAsync(ct);
        }

        if (staleJobIds.Count == 0)
        {
            LogNoStaleJobs(_logger);
            return;
        }

        LogStaleJobsFound(_logger, staleJobIds.Count);

        foreach (var jobId in staleJobIds)
        {
            try
            {
                await ProcessJobAsync(jobId, ct);
            }
            catch (Exception ex)
            {
                LogJobProcessingFailed(_logger, ex, jobId);
            }
        }
    }

    public async Task ProcessJobAsync(Guid jobId, CancellationToken ct)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<JobDbContext>();

        var job = await db.Jobs
            .Include(j => j.Photos)
            .FirstOrDefaultAsync(j => j.Id == jobId, ct);

        if (job is null)
        {
            LogJobNotFound(_logger, jobId);
            return;
        }

        // Re-check status inside the new scope — another process may have updated it
        if (job.Status != JobStatus.Printing)
            return;

        if (job.RetryCount >= _options.MaxRetries)
            await MarkAsErrorAsync(db, job, ct);
        else
            await RequeueJobAsync(db, job, ct);
    }

    private async Task MarkAsErrorAsync(JobDbContext db, Job job, CancellationToken ct)
    {
        job.Status = JobStatus.Error;
        job.Error = "max retries reached";
        job.UpdatedAt = DateTimeOffset.UtcNow;

        if (db.Database.IsRelational())
        {
            await using var tx = await db.Database.BeginTransactionAsync(ct);
            await db.SaveChangesAsync(ct);
            await tx.CommitAsync(ct);
        }
        else
        {
            await db.SaveChangesAsync(ct);
        }

        await PublishBroadcastAsync(job, ct);
        LogJobMarkedAsError(_logger, job.Id);
    }

    private async Task RequeueJobAsync(JobDbContext db, Job job, CancellationToken ct)
    {
        var startFromIndex = job.Printed;

        await PublishRequeueMessageAsync(job, startFromIndex, ct);

        job.Status = JobStatus.Requeued;
        job.RetryCount++;
        job.UpdatedAt = DateTimeOffset.UtcNow;

        if (db.Database.IsRelational())
        {
            await using var tx = await db.Database.BeginTransactionAsync(ct);
            await db.SaveChangesAsync(ct);
            await tx.CommitAsync(ct);
        }
        else
        {
            await db.SaveChangesAsync(ct);
        }

        await PublishBroadcastAsync(job, ct);
        LogJobRequeued(_logger, job.Id, job.RetryCount);
    }

    private async Task PublishRequeueMessageAsync(Job job, int startFromIndex, CancellationToken ct)
    {
        var connection = await _connectionManager.GetConnectionAsync(ct);
        using var channel = await connection.CreateChannelAsync(cancellationToken: ct);

        var message = new
        {
            JobId = job.Id,
            Photos = job.Photos.Select(p => new { p.PhotoStorageKey, p.Copies }),
            StartFromIndex = startFromIndex
        };

        var body = JsonSerializer.SerializeToUtf8Bytes(message, SerializeOptions);

        await channel.BasicPublishAsync(
            exchange: RabbitMqInitializer.ExchangeName,
            routingKey: RabbitMqInitializer.PrintJobsQueue,
            body: body,
            cancellationToken: ct);
    }

    private async Task PublishBroadcastAsync(Job job, CancellationToken ct)
    {
        var message = new PrintStatusMessage(
            job.Id,
            job.Status.ToString().ToLower(),
            job.Printed,
            job.Total,
            job.Error);

        var connection = await _connectionManager.GetConnectionAsync(ct);
        using var channel = await connection.CreateChannelAsync(cancellationToken: ct);
        var body = JsonSerializer.SerializeToUtf8Bytes(message, SerializeOptions);
        await channel.BasicPublishAsync(
            exchange: RabbitMqInitializer.BroadcastExchangeName,
            routingKey: "",
            body: body,
            cancellationToken: ct);
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Watchdog found no stale jobs.")]
    private static partial void LogNoStaleJobs(ILogger logger);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Watchdog found {Count} stale job(s). Processing...")]
    private static partial void LogStaleJobsFound(ILogger logger, int count);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Job {JobId} reached max retries. Marking as error.")]
    private static partial void LogJobMarkedAsError(ILogger logger, Guid jobId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Job {JobId} requeued (retry {RetryCount}).")]
    private static partial void LogJobRequeued(ILogger logger, Guid jobId, int retryCount);

    [LoggerMessage(Level = LogLevel.Error, Message = "Watchdog run failed.")]
    private static partial void LogRunFailed(ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to process stale job {JobId}. Skipping.")]
    private static partial void LogJobProcessingFailed(ILogger logger, Exception ex, Guid jobId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Stale job {JobId} not found — may have been deleted. Skipping.")]
    private static partial void LogJobNotFound(ILogger logger, Guid jobId);
}
