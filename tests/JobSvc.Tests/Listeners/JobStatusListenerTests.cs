using System.Text.Json;
using System.Threading.Channels;
using JobSvc.Data;
using JobSvc.Listeners;
using JobSvc.Models;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace JobSvc.Tests.Listeners;

public class JobStatusListenerTests
{
    private static (JobStatusListener Listener, Channel<StatusUpdate> Channel) CreateListener()
    {
        var channel = Channel.CreateUnbounded<StatusUpdate>();
        var listener = new JobStatusListener(
            Mock.Of<INpgsqlConnectionFactory>(),
            channel.Writer,
            NullLogger<JobStatusListener>.Instance);
        return (listener, channel);
    }

    private static string Serialize(object payload) =>
        JsonSerializer.Serialize(payload, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    [Fact]
    public void HandleNotification_ValidPayload_WritesToChannel()
    {
        var (listener, channel) = CreateListener();
        var payload = Serialize(new { jobId = "abc", status = "printing", printed = 1, total = 3, error = (string?)null });

        listener.HandleNotification(payload);

        Assert.True(channel.Reader.TryRead(out var update));
        Assert.NotNull(update);
        Assert.Equal("abc", update.JobId);
        Assert.Equal("printing", update.Status);
        Assert.Equal(1, update.Printed);
        Assert.Equal(3, update.Total);
        Assert.Null(update.Error);
    }

    [Fact]
    public void HandleNotification_WithError_WritesErrorField()
    {
        var (listener, channel) = CreateListener();
        var payload = Serialize(new { jobId = "xyz", status = "error", printed = 2, total = 5, error = "Printer jammed" });

        listener.HandleNotification(payload);

        Assert.True(channel.Reader.TryRead(out var update));
        Assert.Equal("error", update!.Status);
        Assert.Equal("Printer jammed", update.Error);
    }

    [Fact]
    public void HandleNotification_InvalidJson_DoesNotWrite()
    {
        var (listener, channel) = CreateListener();

        listener.HandleNotification("not valid json {{");

        Assert.False(channel.Reader.TryRead(out _));
    }

    [Fact]
    public void HandleNotification_NullDeserialization_DoesNotWrite()
    {
        var (listener, channel) = CreateListener();

        listener.HandleNotification("null");

        Assert.False(channel.Reader.TryRead(out _));
    }

    [Fact]
    public void HandleNotification_MultipleNotifications_AllWritten()
    {
        var (listener, channel) = CreateListener();

        listener.HandleNotification(Serialize(new { jobId = "j1", status = "printing", printed = 1, total = 3, error = (string?)null }));
        listener.HandleNotification(Serialize(new { jobId = "j2", status = "done", printed = 3, total = 3, error = (string?)null }));

        Assert.Equal(2, channel.Reader.Count);
    }

    [Fact]
    public void HandleNotification_StatusCaseInsensitive_Succeeds()
    {
        var (listener, channel) = CreateListener();
        var payload = Serialize(new { jobId = "abc", status = "Printing", printed = 1, total = 2, error = (string?)null });

        listener.HandleNotification(payload);

        Assert.True(channel.Reader.TryRead(out var update));
        Assert.Equal("Printing", update!.Status);
    }
}
