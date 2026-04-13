using System.Text.Json;
using JobSvc.Data;
using JobSvc.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);

// Register and validate database configuration
builder.Services.AddOptions<DatabaseOptions>()
    .Bind(builder.Configuration.GetSection(DatabaseOptions.SectionName))
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddDbContext<JobDbContext>((sp, options) =>
{
    var dbOptions = sp.GetRequiredService<IOptions<DatabaseOptions>>().Value;
    options.UseNpgsql(dbOptions.CockroachDb);
});

// Register RabbitMQ options and manager
builder.Services.AddOptions<RabbitMqOptions>()
    .Bind(builder.Configuration.GetSection(RabbitMqOptions.SectionName))
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddSingleton<IConnectionFactory>(sp =>
{
    var options = sp.GetRequiredService<IOptions<RabbitMqOptions>>().Value;
    return new ConnectionFactory
    {
        Uri = new Uri(options.Uri),
        AutomaticRecoveryEnabled = true
    };
});
builder.Services.AddSingleton<IRabbitMqConnectionManager, RabbitMqConnectionManager>();
builder.Services.AddSingleton<IRabbitMqInitializer, RabbitMqInitializer>();

var app = builder.Build();

// Ensure database is migrated on startup with retries (matching ingest-svc's robustness)
using (var scope = app.Services.CreateScope())
{
    var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();
    var db = scope.ServiceProvider.GetRequiredService<JobDbContext>();
    var rabbitInitializer = scope.ServiceProvider.GetRequiredService<IRabbitMqInitializer>();
    
    var retryCount = 0;
    const int maxRetries = 10;
    var delay = TimeSpan.FromSeconds(2);

    while (true)
    {
        try
        {
            logger.LogInformation("Applying database migrations (attempt {Attempt}/{MaxRetries})...", retryCount + 1, maxRetries);
            if (db.Database.IsRelational())
                await db.Database.MigrateAsync();
            else
                await db.Database.EnsureCreatedAsync();
            logger.LogInformation("Database migrations applied successfully.");
            
            await rabbitInitializer.InitializeAsync();
            break;
        }
        catch (Exception ex) when (retryCount < maxRetries)
        {
            retryCount++;
            logger.LogWarning(ex, "Startup initialization attempt {Attempt} failed. Retrying in {Delay}...", retryCount, delay);
            await Task.Delay(delay);
            delay = delay * 2; // Exponential backoff
        }
    }
}

app.MapGet("/health", () => Results.Ok());

app.MapPost("/jobs", async (
    [FromBody] CreateJobRequest request,
    JobDbContext db,
    IRabbitMqConnectionManager rabbitManager,
    ILogger<Program> logger) =>
{
    if (request.Photos.Count == 0)
    {
        return Results.BadRequest("At least one photo is required.");
    }

    var job = new Job
    {
        Total = request.Photos.Count,
        Status = JobStatus.Queued,
        Photos = request.Photos.Select(p => new JobPhoto
        {
            PhotoStorageKey = p.PhotoStorageKey,
            Copies = p.Copies
        }).ToList()
    };

    db.Jobs.Add(job);
    await db.SaveChangesAsync();

    try
    {
        var connection = await rabbitManager.GetConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        var message = new
        {
            JobId = job.Id,
            Photos = request.Photos,
            StartFromIndex = 0
        };

        var body = JsonSerializer.SerializeToUtf8Bytes(message);

        await channel.BasicPublishAsync(
            exchange: RabbitMqInitializer.ExchangeName,
            routingKey: RabbitMqInitializer.PrintJobsQueue,
            body: body);

        logger.LogInformation("Job {JobId} created and published to RabbitMQ.", job.Id);
    }
    catch (Exception ex)
    {
        // Even if RabbitMQ publication fails, the job is in DB. 
        // In a real scenario, we might want to handle this with an outbox pattern,
        // but for this issue, we just log it.
        logger.LogError(ex, "Failed to publish job {JobId} to RabbitMQ.", job.Id);
    }

    return Results.Created($"/jobs/{job.Id}", new JobResponse(job.Id, job.Status.ToString().ToLower(), job.CreatedAt));
});

app.Run();

public partial class Program { }
