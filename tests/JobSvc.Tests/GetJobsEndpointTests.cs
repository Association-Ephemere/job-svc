using System.Net;
using System.Net.Http.Json;
using JobSvc.Data;
using JobSvc.Models;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using RabbitMQ.Client;

namespace JobSvc.Tests;

public class GetJobsEndpointTests : IDisposable
{
    private record JobListResponse(List<JobListItemDto> Jobs, int Total);
    private record JobListItemDto(Guid Id, string Status, int Total, int Printed, int RetryCount, DateTimeOffset CreatedAt, DateTimeOffset UpdatedAt);
    private record JobDetailResponse(Guid Id, string Status, int Total, int Printed, int RetryCount, DateTimeOffset CreatedAt, DateTimeOffset UpdatedAt, List<PhotoDto> Photos);
    private record PhotoDto(string PhotoStorageKey, int Copies);

    private readonly List<WebApplicationFactory<Program>> _factories = [];

    public void Dispose()
    {
        foreach (var factory in _factories)
            factory.Dispose();
    }

    private WebApplicationFactory<Program> CreateFactory()
    {
        var dbName = "TestDb_" + Guid.NewGuid();

        var rabbitManagerMock = new Mock<IRabbitMqConnectionManager>();
        var rabbitInitializerMock = new Mock<IRabbitMqInitializer>();
        rabbitInitializerMock
            .Setup(i => i.InitializeAsync(It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            builder.UseSetting("ConnectionStrings:CockroachDb", "Host=localhost");
            builder.UseSetting("RabbitMq:Uri", "amqp://guest:guest@localhost/");

            builder.ConfigureServices(services =>
            {
                var dbDescriptor = services.Single(d => d.ServiceType == typeof(DbContextOptions<JobDbContext>));
                services.Remove(dbDescriptor);
                services.AddDbContext<JobDbContext>(options => options.UseInMemoryDatabase(dbName));

                var managerDescriptor = services.Single(d => d.ServiceType == typeof(IRabbitMqConnectionManager));
                services.Remove(managerDescriptor);
                services.AddSingleton<IRabbitMqConnectionManager>(_ => rabbitManagerMock.Object);

                var initDescriptor = services.Single(d => d.ServiceType == typeof(IRabbitMqInitializer));
                services.Remove(initDescriptor);
                services.AddSingleton<IRabbitMqInitializer>(_ => rabbitInitializerMock.Object);
            });
        });

        _factories.Add(factory);
        return factory;
    }

    private static async Task SeedAsync(WebApplicationFactory<Program> factory, params Job[] jobs)
    {
        using var scope = factory.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<JobDbContext>();
        db.Jobs.AddRange(jobs);
        await db.SaveChangesAsync();
    }

    [Fact]
    public async Task GetJobs_NoFilter_ReturnsAllJobs()
    {
        var factory = CreateFactory();
        await SeedAsync(factory,
            new Job { Total = 2, Status = JobStatus.Queued },
            new Job { Total = 1, Status = JobStatus.Printing });

        var response = await factory.CreateClient().GetAsync("/jobs");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadFromJsonAsync<JobListResponse>();
        Assert.NotNull(body);
        Assert.Equal(2, body.Total);
        Assert.Equal(2, body.Jobs.Count);
    }

    [Fact]
    public async Task GetJobs_StatusFilter_ReturnsMatchingOnly()
    {
        var factory = CreateFactory();
        await SeedAsync(factory,
            new Job { Total = 1, Status = JobStatus.Queued },
            new Job { Total = 1, Status = JobStatus.Printing },
            new Job { Total = 1, Status = JobStatus.Done });

        var response = await factory.CreateClient().GetAsync("/jobs?status=queued,printing");

        var body = await response.Content.ReadFromJsonAsync<JobListResponse>();
        Assert.NotNull(body);
        Assert.Equal(2, body.Total);
        Assert.All(body.Jobs, j => Assert.Contains(j.Status, new[] { "queued", "printing" }));
    }

    [Fact]
    public async Task GetJobs_LimitOffset_Paginates()
    {
        var factory = CreateFactory();
        await SeedAsync(factory,
            new Job { Total = 1, Status = JobStatus.Queued },
            new Job { Total = 1, Status = JobStatus.Queued },
            new Job { Total = 1, Status = JobStatus.Queued });

        var response = await factory.CreateClient().GetAsync("/jobs?limit=2&offset=1");

        var body = await response.Content.ReadFromJsonAsync<JobListResponse>();
        Assert.NotNull(body);
        Assert.Equal(3, body.Total);
        Assert.Equal(2, body.Jobs.Count);
    }

    [Fact]
    public async Task GetJobs_SortByStatus_ReturnsCorrectOrder()
    {
        var factory = CreateFactory();
        await SeedAsync(factory,
            new Job { Total = 1, Status = JobStatus.Done },
            new Job { Total = 1, Status = JobStatus.Queued },
            new Job { Total = 1, Status = JobStatus.Printing },
            new Job { Total = 1, Status = JobStatus.Error },
            new Job { Total = 1, Status = JobStatus.Requeued });

        var response = await factory.CreateClient().GetAsync("/jobs?sort=status");

        var body = await response.Content.ReadFromJsonAsync<JobListResponse>();
        Assert.NotNull(body);
        Assert.Equal(["printing", "requeued", "queued", "error", "done"],
            body.Jobs.Select(j => j.Status).ToList());
    }

    [Fact]
    public async Task GetJobs_ResponseShape_CorrectFields()
    {
        var factory = CreateFactory();
        await SeedAsync(factory, new Job { Total = 3, Printed = 1, RetryCount = 0, Status = JobStatus.Printing });

        var response = await factory.CreateClient().GetAsync("/jobs");

        var body = await response.Content.ReadFromJsonAsync<JobListResponse>();
        Assert.NotNull(body);
        var job = Assert.Single(body.Jobs);
        Assert.NotEqual(Guid.Empty, job.Id);
        Assert.Equal("printing", job.Status);
        Assert.Equal(3, job.Total);
        Assert.Equal(1, job.Printed);
        Assert.Equal(0, job.RetryCount);
    }

    [Fact]
    public async Task GetJobById_ReturnsJobWithPhotos()
    {
        var factory = CreateFactory();
        var job = new Job
        {
            Total = 2,
            Status = JobStatus.Queued,
            Photos =
            [
                new JobPhoto { PhotoStorageKey = "full/abc.jpg", Copies = 2 },
                new JobPhoto { PhotoStorageKey = "full/def.jpg", Copies = 1 }
            ]
        };
        await SeedAsync(factory, job);

        var response = await factory.CreateClient().GetAsync($"/jobs/{job.Id}");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadFromJsonAsync<JobDetailResponse>();
        Assert.NotNull(body);
        Assert.Equal(job.Id, body.Id);
        Assert.Equal("queued", body.Status);
        Assert.Equal(2, body.Photos.Count);
        Assert.Contains(body.Photos, p => p.PhotoStorageKey == "full/abc.jpg" && p.Copies == 2);
        Assert.Contains(body.Photos, p => p.PhotoStorageKey == "full/def.jpg" && p.Copies == 1);
    }

    [Fact]
    public async Task GetJobById_UnknownId_Returns404()
    {
        var factory = CreateFactory();

        var response = await factory.CreateClient().GetAsync($"/jobs/{Guid.NewGuid()}");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }
}
