using System.Net;
using System.Net.Http.Json;
using JobSvc.Data;
using JobSvc.Models;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using RabbitMQ.Client;
using Npgsql;

namespace JobSvc.Tests;

public class JobsEndpointTests : IDisposable
{
    private readonly List<WebApplicationFactory<Program>> _factories = [];

    public void Dispose()
    {
        foreach (var factory in _factories)
            factory.Dispose();
    }

    private WebApplicationFactory<Program> CreateFactory(out Mock<IChannel> channelMock)
    {
        var localChannelMock = new Mock<IChannel>();
        channelMock = localChannelMock;

        var connectionMock = new Mock<IConnection>();
        connectionMock
            .Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(localChannelMock.Object);

        var rabbitManagerMock = new Mock<IRabbitMqConnectionManager>();
        rabbitManagerMock
            .Setup(m => m.GetConnectionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(connectionMock.Object);

        var rabbitInitializerMock = new Mock<IRabbitMqInitializer>();
        rabbitInitializerMock
            .Setup(i => i.InitializeAsync(It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var dbName = "TestDb_" + Guid.NewGuid();

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

                var npgsqlFactoryDescriptor = services.Single(d => d.ServiceType == typeof(INpgsqlConnectionFactory));
                services.Remove(npgsqlFactoryDescriptor);
                var npgsqlFactoryMock = new Mock<INpgsqlConnectionFactory>();
                npgsqlFactoryMock.Setup(f => f.Create()).Throws(new NpgsqlException("Test: DB not available"));
                services.AddSingleton<INpgsqlConnectionFactory>(_ => npgsqlFactoryMock.Object);
            });
        });
        _factories.Add(factory);
        return factory;
    }

    [Fact]
    public async Task CreateJob_WithValidRequest_Returns201WithJobResponse()
    {
        var factory = CreateFactory(out _);
        var client = factory.CreateClient();

        var request = new CreateJobRequest(
        [
            new PhotoRequest("full/abc.jpg", 2),
            new PhotoRequest("full/def.jpg", 1)
        ]);

        var response = await client.PostAsJsonAsync("/jobs", request);

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        var body = await response.Content.ReadFromJsonAsync<JobResponse>();
        Assert.NotNull(body);
        Assert.NotEqual(Guid.Empty, body.JobId);
        Assert.Equal("queued", body.Status);
        Assert.True(body.CreatedAt > DateTimeOffset.MinValue);
    }

    [Fact]
    public async Task CreateJob_WithEmptyPhotos_Returns400()
    {
        var factory = CreateFactory(out _);
        var client = factory.CreateClient();

        var response = await client.PostAsJsonAsync("/jobs", new CreateJobRequest([]));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task CreateJob_InsertsJobAndPhotosInDb()
    {
        var factory = CreateFactory(out _);
        var client = factory.CreateClient();

        var request = new CreateJobRequest(
        [
            new PhotoRequest("full/abc.jpg", 2),
            new PhotoRequest("full/def.jpg", 1)
        ]);

        var response = await client.PostAsJsonAsync("/jobs", request);
        Assert.Equal(HttpStatusCode.Created, response.StatusCode);

        using var scope = factory.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<JobDbContext>();
        var jobs = await db.Jobs.Include(j => j.Photos).ToListAsync();

        Assert.Single(jobs);
        Assert.Equal(2, jobs[0].Total);
        Assert.Equal(2, jobs[0].Photos.Count);
        Assert.Contains(jobs[0].Photos, p => p.PhotoStorageKey == "full/abc.jpg" && p.Copies == 2);
        Assert.Contains(jobs[0].Photos, p => p.PhotoStorageKey == "full/def.jpg" && p.Copies == 1);
    }

    [Fact]
    public async Task CreateJob_PublishesMessageToRabbitMq()
    {
        var factory = CreateFactory(out var channelMock);
        var client = factory.CreateClient();
        var request = new CreateJobRequest([new PhotoRequest("full/abc.jpg", 2)]);

        await client.PostAsJsonAsync("/jobs", request);

        var publishInvocation = channelMock.Invocations.SingleOrDefault(inv =>
            inv.Method.Name == nameof(IChannel.BasicPublishAsync) &&
            inv.Arguments[0] is string exchange && exchange == RabbitMqInitializer.ExchangeName &&
            inv.Arguments[1] is string routingKey && routingKey == RabbitMqInitializer.PrintJobsQueue);

        Assert.NotNull(publishInvocation);
    }
}
