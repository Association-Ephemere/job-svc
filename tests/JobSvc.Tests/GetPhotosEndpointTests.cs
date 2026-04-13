using System.Net;
using System.Net.Http.Json;
using JobSvc.Data;
using JobSvc.Models;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Minio;
using Minio.ApiEndpoints;
using Minio.DataModel;
using Minio.DataModel.Args;
using Moq;
using Npgsql;
using RabbitMQ.Client;

namespace JobSvc.Tests;

public class GetPhotosEndpointTests : IDisposable
{
    private record PhotoListResponse(List<PhotoEntry> Photos, int Total);
    private record PhotoEntry(string Key, string Url);

    private readonly List<WebApplicationFactory<Program>> _factories = [];

    public void Dispose()
    {
        foreach (var factory in _factories)
            factory.Dispose();
    }

    private WebApplicationFactory<Program> CreateFactory(
        IEnumerable<Item>? minioItems = null,
        string presignedUrlBase = "https://minio.example.com")
    {
        var items = (minioItems ?? []).ToList();

        var bucketMock = new Mock<IBucketOperations>();
        bucketMock
            .Setup(b => b.ListObjectsEnumAsync(It.IsAny<ListObjectsArgs>(), It.IsAny<CancellationToken>()))
            .Returns(AsyncEnumerable(items));

        var objectsMock = new Mock<IObjectOperations>();
        objectsMock
            .Setup(o => o.PresignedGetObjectAsync(It.IsAny<PresignedGetObjectArgs>()))
            .ReturnsAsync($"{presignedUrlBase}/photo.jpg");

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

        var dbName = "TestDb_" + Guid.NewGuid();

        var factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            builder.UseSetting("ConnectionStrings:Postgres", "Host=localhost");
            builder.UseSetting("RabbitMq:Uri", "amqp://guest:guest@localhost/");
            builder.UseSetting("MinIO:Endpoint", "localhost:9000");
            builder.UseSetting("MinIO:AccessKey", "minioadmin");
            builder.UseSetting("MinIO:SecretKey", "minioadmin");
            builder.UseSetting("MinIO:Bucket", "photostand");

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

                // Replace MinIO services
                services.Remove(services.Single(d => d.ServiceType == typeof(IMinioClient)));
                services.Remove(services.Single(d => d.ServiceType == typeof(IBucketOperations)));
                services.Remove(services.Single(d => d.ServiceType == typeof(IObjectOperations)));
                services.AddSingleton<IBucketOperations>(_ => bucketMock.Object);
                services.AddSingleton<IObjectOperations>(_ => objectsMock.Object);
                services.AddSingleton<IMinioClient>(_ => Mock.Of<IMinioClient>());
            });
        });

        _factories.Add(factory);
        return factory;
    }

    private static Item MakeItem(string key, bool isDir = false) =>
        new() { Key = key, IsDir = isDir };

#pragma warning disable CS1998
    private static async IAsyncEnumerable<T> AsyncEnumerable<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
            yield return item;
    }
#pragma warning restore CS1998

    [Fact]
    public async Task GetPhotos_EmptyBucket_ReturnsEmptyList()
    {
        var factory = CreateFactory([]);
        var response = await factory.CreateClient().GetAsync("/photos");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadFromJsonAsync<PhotoListResponse>();
        Assert.NotNull(body);
        Assert.Empty(body.Photos);
        Assert.Equal(0, body.Total);
    }

    [Fact]
    public async Task GetPhotos_WithObjects_ReturnsPhotoList()
    {
        var factory = CreateFactory([
            MakeItem("low/a.jpg"),
            MakeItem("low/b.jpg"),
        ]);

        var response = await factory.CreateClient().GetAsync("/photos");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadFromJsonAsync<PhotoListResponse>();
        Assert.NotNull(body);
        Assert.Equal(2, body.Total);
        Assert.Equal(2, body.Photos.Count);
    }

    [Fact]
    public async Task GetPhotos_IncludesPresignedUrls()
    {
        var factory = CreateFactory(
            [MakeItem("low/a.jpg")],
            presignedUrlBase: "https://minio.example.com");

        var body = await (await factory.CreateClient().GetAsync("/photos"))
            .Content.ReadFromJsonAsync<PhotoListResponse>();

        Assert.NotNull(body);
        var photo = Assert.Single(body.Photos);
        Assert.Equal("low/a.jpg", photo.Key);
        Assert.NotEmpty(photo.Url);
    }

    [Fact]
    public async Task GetPhotos_FiltersDirectoryObjects()
    {
        var factory = CreateFactory([
            MakeItem("low/", isDir: true),
            MakeItem("low/a.jpg"),
        ]);

        var body = await (await factory.CreateClient().GetAsync("/photos"))
            .Content.ReadFromJsonAsync<PhotoListResponse>();

        Assert.NotNull(body);
        Assert.Equal(1, body.Total);
        Assert.DoesNotContain(body.Photos, p => p.Key.EndsWith('/'));
    }

    [Fact]
    public async Task GetPhotos_LimitAndOffset_ReturnCorrectSlice()
    {
        var factory = CreateFactory([
            MakeItem("low/a.jpg"),
            MakeItem("low/b.jpg"),
            MakeItem("low/c.jpg"),
            MakeItem("low/d.jpg"),
        ]);

        var body = await (await factory.CreateClient().GetAsync("/photos?limit=2&offset=1"))
            .Content.ReadFromJsonAsync<PhotoListResponse>();

        Assert.NotNull(body);
        Assert.Equal(4, body.Total);
        Assert.Equal(2, body.Photos.Count);
        Assert.Equal("low/b.jpg", body.Photos[0].Key);
        Assert.Equal("low/c.jpg", body.Photos[1].Key);
    }

    [Fact]
    public async Task GetPhotos_DefaultLimit50_WhenNotSpecified()
    {
        var items = Enumerable.Range(1, 60).Select(i => MakeItem($"low/{i:D3}.jpg"));
        var factory = CreateFactory(items);

        var body = await (await factory.CreateClient().GetAsync("/photos"))
            .Content.ReadFromJsonAsync<PhotoListResponse>();

        Assert.NotNull(body);
        Assert.Equal(60, body.Total);
        Assert.Equal(50, body.Photos.Count);
    }

    [Fact]
    public async Task GetPhotos_PhotosOrderedByKeyAlphabetically()
    {
        var factory = CreateFactory([
            MakeItem("low/c.jpg"),
            MakeItem("low/a.jpg"),
            MakeItem("low/b.jpg"),
        ]);

        var body = await (await factory.CreateClient().GetAsync("/photos"))
            .Content.ReadFromJsonAsync<PhotoListResponse>();

        Assert.NotNull(body);
        Assert.Equal(["low/a.jpg", "low/b.jpg", "low/c.jpg"],
            body.Photos.Select(p => p.Key).ToList());
    }
}
