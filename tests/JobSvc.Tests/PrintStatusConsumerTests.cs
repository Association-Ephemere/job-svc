using JobSvc.Consumers;
using JobSvc.Data;
using JobSvc.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace JobSvc.Tests;

public class PrintStatusConsumerTests
{
    private static (PrintStatusConsumer Consumer, IServiceProvider ServiceProvider) CreateConsumer()
    {
        var services = new ServiceCollection();
        var dbName = "PrintStatusConsumerDb_" + Guid.NewGuid();
        services.AddDbContext<JobDbContext>(opts => opts.UseInMemoryDatabase(dbName));

        var sp = services.BuildServiceProvider();

        var consumer = new PrintStatusConsumer(
            Mock.Of<IRabbitMqConnectionManager>(),
            sp.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<PrintStatusConsumer>.Instance);

        return (consumer, sp);
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
    public async Task HandleMessage_ValidMessage_UpdatesStatusAndPrinted()
    {
        var (consumer, sp) = CreateConsumer();
        var jobId = Guid.NewGuid();
        await SeedJobAsync(sp, new Job { Id = jobId, Status = JobStatus.Queued, Total = 3 });

        var message = new PrintStatusMessage(jobId, "printing", 1, 3, null);
        await consumer.HandleMessageAsync(message, CancellationToken.None);

        var job = await GetJobAsync(sp, jobId);
        Assert.NotNull(job);
        Assert.Equal(JobStatus.Printing, job.Status);
        Assert.Equal(1, job.Printed);
        Assert.Null(job.Error);
    }

    [Fact]
    public async Task HandleMessage_WithError_SetsErrorField()
    {
        var (consumer, sp) = CreateConsumer();
        var jobId = Guid.NewGuid();
        await SeedJobAsync(sp, new Job { Id = jobId, Status = JobStatus.Printing, Total = 3 });

        var message = new PrintStatusMessage(jobId, "error", 1, 3, "Printer jammed");
        await consumer.HandleMessageAsync(message, CancellationToken.None);

        var job = await GetJobAsync(sp, jobId);
        Assert.NotNull(job);
        Assert.Equal(JobStatus.Error, job.Status);
        Assert.Equal("Printer jammed", job.Error);
    }

    [Fact]
    public async Task HandleMessage_DoneStatus_SetsPrinted()
    {
        var (consumer, sp) = CreateConsumer();
        var jobId = Guid.NewGuid();
        await SeedJobAsync(sp, new Job { Id = jobId, Status = JobStatus.Printing, Total = 5 });

        var message = new PrintStatusMessage(jobId, "done", 5, 5, null);
        await consumer.HandleMessageAsync(message, CancellationToken.None);

        var job = await GetJobAsync(sp, jobId);
        Assert.NotNull(job);
        Assert.Equal(JobStatus.Done, job.Status);
        Assert.Equal(5, job.Printed);
    }

    [Fact]
    public async Task HandleMessage_StatusCaseInsensitive_Succeeds()
    {
        var (consumer, sp) = CreateConsumer();
        var jobId = Guid.NewGuid();
        await SeedJobAsync(sp, new Job { Id = jobId, Status = JobStatus.Queued, Total = 2 });

        var message = new PrintStatusMessage(jobId, "Printing", 1, 2, null);
        await consumer.HandleMessageAsync(message, CancellationToken.None);

        var job = await GetJobAsync(sp, jobId);
        Assert.Equal(JobStatus.Printing, job!.Status);
    }

    [Fact]
    public async Task HandleMessage_UnknownStatus_ThrowsArgumentException()
    {
        var (consumer, sp) = CreateConsumer();
        var jobId = Guid.NewGuid();
        await SeedJobAsync(sp, new Job { Id = jobId, Status = JobStatus.Queued, Total = 1 });

        var message = new PrintStatusMessage(jobId, "flying", 0, 1, null);
        await Assert.ThrowsAsync<ArgumentException>(() =>
            consumer.HandleMessageAsync(message, CancellationToken.None));
    }

    [Fact]
    public async Task HandleMessage_JobNotFound_ThrowsInvalidOperationException()
    {
        var (consumer, sp) = CreateConsumer();

        var message = new PrintStatusMessage(Guid.NewGuid(), "printing", 1, 3, null);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            consumer.HandleMessageAsync(message, CancellationToken.None));
    }

    [Fact]
    public async Task HandleMessage_UpdatesUpdatedAt()
    {
        var (consumer, sp) = CreateConsumer();
        var jobId = Guid.NewGuid();
        var originalUpdatedAt = DateTimeOffset.UtcNow.AddMinutes(-5);
        await SeedJobAsync(sp, new Job
        {
            Id = jobId,
            Status = JobStatus.Queued,
            Total = 2,
            UpdatedAt = originalUpdatedAt
        });

        var message = new PrintStatusMessage(jobId, "printing", 1, 2, null);
        await consumer.HandleMessageAsync(message, CancellationToken.None);

        var job = await GetJobAsync(sp, jobId);
        Assert.True(job!.UpdatedAt > originalUpdatedAt);
    }
}
