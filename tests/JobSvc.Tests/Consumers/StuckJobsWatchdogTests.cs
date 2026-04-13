using JobSvc.Consumers;
using JobSvc.Data;
using JobSvc.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;

namespace JobSvc.Tests.Consumers;

public class StuckJobsWatchdogTests
{
    private static (StuckJobsWatchdog Watchdog, IServiceProvider ServiceProvider, Mock<IChannel> ChannelMock) Create(
        int maxRetries = 3,
        int staleThresholdMinutes = 5)
    {
        var services = new ServiceCollection();
        var dbName = "WatchdogDb_" + Guid.NewGuid();
        services.AddDbContext<JobDbContext>(opts => opts.UseInMemoryDatabase(dbName));

        var sp = services.BuildServiceProvider();

        var channelMock = new Mock<IChannel>();

        var connectionMock = new Mock<IConnection>();
        connectionMock
            .Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(channelMock.Object);

        var rabbitManagerMock = new Mock<IRabbitMqConnectionManager>();
        rabbitManagerMock
            .Setup(m => m.GetConnectionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(connectionMock.Object);

        var options = Options.Create(new WatchdogOptions
        {
            IntervalMinutes = 1,
            StaleThresholdMinutes = staleThresholdMinutes,
            MaxRetries = maxRetries
        });

        var watchdog = new StuckJobsWatchdog(
            rabbitManagerMock.Object,
            sp.GetRequiredService<IServiceScopeFactory>(),
            options,
            NullLogger<StuckJobsWatchdog>.Instance);

        return (watchdog, sp, channelMock);
    }

    private static async Task SeedJobAsync(IServiceProvider sp, Job job)
    {
        await using var scope = sp.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<JobDbContext>();
        db.Jobs.Add(job);
        await db.SaveChangesAsync();
    }

    private static async Task<Job?> GetJobAsync(IServiceProvider sp, Guid jobId)
    {
        await using var scope = sp.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<JobDbContext>();
        return await db.Jobs.AsNoTracking().FirstOrDefaultAsync(j => j.Id == jobId);
    }

    [Fact]
    public async Task RunAsync_FreshPrintingJob_NotTouched()
    {
        var (watchdog, sp, channelMock) = Create();
        var jobId = Guid.NewGuid();
        await SeedJobAsync(sp, new Job
        {
            Id = jobId,
            Status = JobStatus.Printing,
            Total = 3,
            UpdatedAt = DateTimeOffset.UtcNow // fresh — below stale threshold
        });

        await watchdog.RunAsync(CancellationToken.None);

        var job = await GetJobAsync(sp, jobId);
        Assert.Equal(JobStatus.Printing, job!.Status);
        channelMock.Verify(c => c.BasicPublishAsync(
            It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
            It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ProcessJobAsync_StatusRecheckSkipsNonPrinting()
    {
        var (watchdog, sp, channelMock) = Create();
        var jobId = Guid.NewGuid();
        // Simulate job that was already updated by another process between query and ProcessJob
        await SeedJobAsync(sp, new Job
        {
            Id = jobId,
            Status = JobStatus.Done,
            Total = 3,
            UpdatedAt = DateTimeOffset.UtcNow.AddHours(-1)
        });

        await watchdog.ProcessJobAsync(jobId, CancellationToken.None);

        var job = await GetJobAsync(sp, jobId);
        Assert.Equal(JobStatus.Done, job!.Status);
        channelMock.Verify(c => c.BasicPublishAsync(
            It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
            It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ProcessJob_NonPrintingJob_NotTouched()
    {
        var (watchdog, sp, _) = Create();
        var jobId = Guid.NewGuid();
        await SeedJobAsync(sp, new Job
        {
            Id = jobId,
            Status = JobStatus.Queued,
            Total = 3,
            UpdatedAt = DateTimeOffset.UtcNow.AddHours(-1)
        });

        await watchdog.ProcessJobAsync(jobId, CancellationToken.None);

        var job = await GetJobAsync(sp, jobId);
        Assert.Equal(JobStatus.Queued, job!.Status);
    }

    [Fact]
    public async Task ProcessJob_StaleJobBelowMaxRetries_Requeued()
    {
        var (watchdog, sp, _) = Create(maxRetries: 3);
        var jobId = Guid.NewGuid();
        await SeedJobAsync(sp, new Job
        {
            Id = jobId,
            Status = JobStatus.Printing,
            Total = 5,
            Printed = 2,
            RetryCount = 1,
            UpdatedAt = DateTimeOffset.UtcNow.AddHours(-1),
            Photos = [new JobPhoto { PhotoStorageKey = "key/a.jpg", Copies = 1 }]
        });

        await watchdog.ProcessJobAsync(jobId, CancellationToken.None);

        var job = await GetJobAsync(sp, jobId);
        Assert.Equal(JobStatus.Requeued, job!.Status);
        Assert.Equal(2, job.RetryCount);
        Assert.Null(job.Error);
    }

    [Fact]
    public async Task ProcessJob_StaleJobBelowMaxRetries_PublishesWithCorrectRoutingKey()
    {
        var (watchdog, sp, channelMock) = Create(maxRetries: 3);
        var jobId = Guid.NewGuid();
        await SeedJobAsync(sp, new Job
        {
            Id = jobId,
            Status = JobStatus.Printing,
            Total = 5,
            Printed = 2,
            RetryCount = 0,
            UpdatedAt = DateTimeOffset.UtcNow.AddHours(-1),
            Photos = [new JobPhoto { PhotoStorageKey = "key/a.jpg", Copies = 1 }]
        });

        await watchdog.ProcessJobAsync(jobId, CancellationToken.None);

        var publishCall = channelMock.Invocations.SingleOrDefault(inv =>
            inv.Method.Name == nameof(IChannel.BasicPublishAsync) &&
            inv.Arguments[0] is string exchange && exchange == RabbitMqInitializer.ExchangeName &&
            inv.Arguments[1] is string routingKey && routingKey == RabbitMqInitializer.PrintJobsQueue);

        Assert.NotNull(publishCall);
    }

    [Fact]
    public async Task ProcessJob_StaleJobAtMaxRetries_MarkedAsError()
    {
        var (watchdog, sp, _) = Create(maxRetries: 3);
        var jobId = Guid.NewGuid();
        await SeedJobAsync(sp, new Job
        {
            Id = jobId,
            Status = JobStatus.Printing,
            Total = 3,
            RetryCount = 3,
            UpdatedAt = DateTimeOffset.UtcNow.AddHours(-1)
        });

        await watchdog.ProcessJobAsync(jobId, CancellationToken.None);

        var job = await GetJobAsync(sp, jobId);
        Assert.Equal(JobStatus.Error, job!.Status);
        Assert.Equal("max retries reached", job.Error);
    }

    [Fact]
    public async Task ProcessJob_StaleJobAboveMaxRetries_MarkedAsErrorWithoutPublish()
    {
        var (watchdog, sp, channelMock) = Create(maxRetries: 3);
        var jobId = Guid.NewGuid();
        await SeedJobAsync(sp, new Job
        {
            Id = jobId,
            Status = JobStatus.Printing,
            Total = 3,
            RetryCount = 5,
            UpdatedAt = DateTimeOffset.UtcNow.AddHours(-1)
        });

        await watchdog.ProcessJobAsync(jobId, CancellationToken.None);

        var job = await GetJobAsync(sp, jobId);
        Assert.Equal(JobStatus.Error, job!.Status);
        channelMock.Verify(c => c.BasicPublishAsync(
            It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
            It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ProcessJob_StaleJobBelowMaxRetries_UpdatesUpdatedAt()
    {
        var (watchdog, sp, _) = Create(maxRetries: 3);
        var jobId = Guid.NewGuid();
        var originalUpdatedAt = DateTimeOffset.UtcNow.AddHours(-1);
        await SeedJobAsync(sp, new Job
        {
            Id = jobId,
            Status = JobStatus.Printing,
            Total = 3,
            RetryCount = 0,
            UpdatedAt = originalUpdatedAt,
            Photos = [new JobPhoto { PhotoStorageKey = "key/a.jpg", Copies = 1 }]
        });

        await watchdog.ProcessJobAsync(jobId, CancellationToken.None);

        var job = await GetJobAsync(sp, jobId);
        Assert.True(job!.UpdatedAt > originalUpdatedAt);
    }

    [Fact]
    public async Task RunAsync_OneJobFails_OtherJobsStillProcessed()
    {
        var services = new ServiceCollection();
        var dbName = "WatchdogDb_" + Guid.NewGuid();
        services.AddDbContext<JobDbContext>(opts => opts.UseInMemoryDatabase(dbName));
        var sp = services.BuildServiceProvider();

        var channelMock = new Mock<IChannel>();
        var connectionMock = new Mock<IConnection>();
        connectionMock
            .Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(channelMock.Object);

        // First call to GetConnectionAsync throws; second call succeeds
        var callCount = 0;
        var rabbitManagerMock = new Mock<IRabbitMqConnectionManager>();
        rabbitManagerMock
            .Setup(m => m.GetConnectionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(() =>
            {
                callCount++;
                if (callCount == 1)
                    throw new InvalidOperationException("RabbitMQ unavailable");
                return connectionMock.Object;
            });

        var options = Options.Create(new WatchdogOptions
        {
            IntervalMinutes = 1,
            StaleThresholdMinutes = 5,
            MaxRetries = 3
        });

        var watchdog = new StuckJobsWatchdog(
            rabbitManagerMock.Object,
            sp.GetRequiredService<IServiceScopeFactory>(),
            options,
            NullLogger<StuckJobsWatchdog>.Instance);

        var staleTime = DateTimeOffset.UtcNow.AddHours(-1);
        var job1Id = Guid.NewGuid();
        var job2Id = Guid.NewGuid();

        await SeedJobAsync(sp, new Job
        {
            Id = job1Id,
            Status = JobStatus.Printing,
            Total = 3,
            RetryCount = 0,
            UpdatedAt = staleTime,
            Photos = [new JobPhoto { PhotoStorageKey = "key/a.jpg", Copies = 1 }]
        });
        await SeedJobAsync(sp, new Job
        {
            Id = job2Id,
            Status = JobStatus.Printing,
            Total = 3,
            RetryCount = 0,
            UpdatedAt = staleTime,
            Photos = [new JobPhoto { PhotoStorageKey = "key/b.jpg", Copies = 1 }]
        });

        await watchdog.RunAsync(CancellationToken.None);

        var job1 = await GetJobAsync(sp, job1Id);
        var job2 = await GetJobAsync(sp, job2Id);

        // One failed (still Printing), one succeeded (Requeued)
        Assert.True(
            (job1!.Status == JobStatus.Printing && job2!.Status == JobStatus.Requeued) ||
            (job1.Status == JobStatus.Requeued && job2!.Status == JobStatus.Printing));
    }

    [Fact]
    public async Task RunAsync_MultipleStaleJobs_AllProcessed()
    {
        var (watchdog, sp, _) = Create(maxRetries: 3);
        var staleTime = DateTimeOffset.UtcNow.AddHours(-1);

        var errorJobId = Guid.NewGuid();
        var requeueJobId = Guid.NewGuid();

        await SeedJobAsync(sp, new Job
        {
            Id = errorJobId,
            Status = JobStatus.Printing,
            Total = 3,
            RetryCount = 3,
            UpdatedAt = staleTime
        });
        await SeedJobAsync(sp, new Job
        {
            Id = requeueJobId,
            Status = JobStatus.Printing,
            Total = 3,
            RetryCount = 1,
            UpdatedAt = staleTime,
            Photos = [new JobPhoto { PhotoStorageKey = "key/b.jpg", Copies = 2 }]
        });

        await watchdog.RunAsync(CancellationToken.None);

        var errorJob = await GetJobAsync(sp, errorJobId);
        var requeueJob = await GetJobAsync(sp, requeueJobId);

        Assert.Equal(JobStatus.Error, errorJob!.Status);
        Assert.Equal(JobStatus.Requeued, requeueJob!.Status);
    }
}
