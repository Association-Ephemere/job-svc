using System.Net;
using System.Text.Json;
using System.Threading.Channels;
using JobSvc.Data;
using JobSvc.Models;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Npgsql;
using RabbitMQ.Client;

namespace JobSvc.Tests;

public class SseEndpointTests : IDisposable
{
    private readonly List<WebApplicationFactory<Program>> _factories = [];

    public void Dispose()
    {
        foreach (var factory in _factories)
            factory.Dispose();
    }

    private WebApplicationFactory<Program> CreateFactory()
    {
        var dbName = "SseTestDb_" + Guid.NewGuid();

        var channelMock = new Mock<IChannel>();
        channelMock
            .Setup(c => c.BasicConsumeAsync(
                It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<string>(),
                It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object?>>(),
                It.IsAny<IAsyncBasicConsumer>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync("consumerTag");

        var connectionMock = new Mock<IConnection>();
        connectionMock
            .Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(channelMock.Object);

        var rabbitManagerMock = new Mock<IRabbitMqConnectionManager>();
        rabbitManagerMock
            .Setup(m => m.GetConnectionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(connectionMock.Object);

        var rabbitInitializerMock = new Mock<IRabbitMqInitializer>();
        rabbitInitializerMock
            .Setup(i => i.InitializeAsync(It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var npgsqlFactoryMock = new Mock<INpgsqlConnectionFactory>();
        npgsqlFactoryMock.Setup(f => f.Create()).Throws(new NpgsqlException("Test: DB not available"));

        var factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            builder.UseSetting("ConnectionStrings:Postgres", "Host=localhost");
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

                var npgsqlDescriptor = services.Single(d => d.ServiceType == typeof(INpgsqlConnectionFactory));
                services.Remove(npgsqlDescriptor);
                services.AddSingleton<INpgsqlConnectionFactory>(_ => npgsqlFactoryMock.Object);
            });
        });

        _factories.Add(factory);
        return factory;
    }

    private static async Task SeedAsync(WebApplicationFactory<Program> factory, Job job)
    {
        using var scope = factory.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<JobDbContext>();
        db.Jobs.Add(job);
        await db.SaveChangesAsync();
    }

    [Fact]
    public async Task Stream_UnknownJob_Returns404()
    {
        var factory = CreateFactory();

        var response = await factory.CreateClient().GetAsync($"/jobs/{Guid.NewGuid()}/stream");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task Stream_DoneJob_SendsInitialEventAndCloses()
    {
        var factory = CreateFactory();
        var job = new Job { Status = JobStatus.Done, Total = 3, Printed = 3 };
        await SeedAsync(factory, job);

        var response = await factory.CreateClient().GetAsync($"/jobs/{job.Id}/stream");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("text/event-stream", response.Content.Headers.ContentType?.MediaType);

        var body = await response.Content.ReadAsStringAsync();
        Assert.StartsWith("data: ", body);
        Assert.Contains("\"status\":\"done\"", body);
        Assert.Contains($"\"jobId\":\"{job.Id}\"", body);
    }

    [Fact]
    public async Task Stream_ErrorJob_SendsInitialEventAndCloses()
    {
        var factory = CreateFactory();
        var job = new Job { Status = JobStatus.Error, Total = 2, Printed = 1, Error = "Jammed" };
        await SeedAsync(factory, job);

        var response = await factory.CreateClient().GetAsync($"/jobs/{job.Id}/stream");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("\"status\":\"error\"", body);
        Assert.Contains("\"error\":\"Jammed\"", body);
    }

    [Fact]
    public async Task Stream_SetsRequiredHeaders()
    {
        var factory = CreateFactory();
        var job = new Job { Status = JobStatus.Done, Total = 1, Printed = 1 };
        await SeedAsync(factory, job);

        var response = await factory.CreateClient().GetAsync($"/jobs/{job.Id}/stream");

        Assert.Equal("text/event-stream", response.Content.Headers.ContentType?.MediaType);
        Assert.Equal("no-cache", response.Headers.GetValues("Cache-Control").First());
        Assert.Equal("no", response.Headers.GetValues("X-Accel-Buffering").First());
    }

    [Fact]
    public async Task Stream_ActiveJob_SendsInitialStateAndForwardsChannelUpdate()
    {
        var factory = CreateFactory();
        var job = new Job { Status = JobStatus.Printing, Total = 3, Printed = 1 };
        await SeedAsync(factory, job);

        var statusChannel = factory.Services.GetRequiredService<Channel<StatusUpdate>>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var client = factory.CreateClient();

        using var response = await client.GetAsync(
            $"/jobs/{job.Id}/stream",
            HttpCompletionOption.ResponseHeadersRead,
            cts.Token);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        using var stream = await response.Content.ReadAsStreamAsync(cts.Token);
        using var reader = new StreamReader(stream);

        // Read initial state event
        var dataLine = await reader.ReadLineAsync(cts.Token);
        await reader.ReadLineAsync(cts.Token); // blank separator line

        Assert.NotNull(dataLine);
        Assert.StartsWith("data: ", dataLine);
        var initial = JsonSerializer.Deserialize<JsonElement>(dataLine!["data: ".Length..]);
        Assert.Equal("printing", initial.GetProperty("status").GetString());
        Assert.Equal(1, initial.GetProperty("printed").GetInt32());

        // Push a done update
        statusChannel.Writer.TryWrite(new StatusUpdate(job.Id.ToString(), "done", 3, 3, null));

        // Read the forwarded event
        var dataLine2 = await reader.ReadLineAsync(cts.Token);
        Assert.NotNull(dataLine2);
        Assert.StartsWith("data: ", dataLine2);
        Assert.Contains("\"status\":\"done\"", dataLine2);
    }

    [Fact]
    public async Task Stream_ActiveJob_IgnoresEventsForOtherJobs()
    {
        var factory = CreateFactory();
        var job = new Job { Status = JobStatus.Printing, Total = 3, Printed = 1 };
        await SeedAsync(factory, job);

        var statusChannel = factory.Services.GetRequiredService<Channel<StatusUpdate>>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var client = factory.CreateClient();

        using var response = await client.GetAsync(
            $"/jobs/{job.Id}/stream",
            HttpCompletionOption.ResponseHeadersRead,
            cts.Token);

        using var stream = await response.Content.ReadAsStreamAsync(cts.Token);
        using var reader = new StreamReader(stream);

        // Consume initial event
        await reader.ReadLineAsync(cts.Token); // data line
        await reader.ReadLineAsync(cts.Token); // blank

        // Push update for a different job, then the real one
        statusChannel.Writer.TryWrite(new StatusUpdate(Guid.NewGuid().ToString(), "done", 3, 3, null));
        statusChannel.Writer.TryWrite(new StatusUpdate(job.Id.ToString(), "done", 3, 3, null));

        // Should eventually receive the matching update
        var dataLine = await reader.ReadLineAsync(cts.Token);
        Assert.Contains("\"status\":\"done\"", dataLine);
        Assert.Contains(job.Id.ToString(), dataLine);
    }
}
