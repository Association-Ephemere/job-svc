using System.Text.Json;
using System.Threading.Channels;
using JobSvc.Consumers;
using JobSvc.Data;
using JobSvc.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Minio;
using Minio.ApiEndpoints;
using Minio.DataModel.Args;
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
    options.UseNpgsql(dbOptions.Postgres);
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
builder.Services.AddHostedService<PrintStatusConsumer>();

builder.Services.AddOptions<WatchdogOptions>()
    .Bind(builder.Configuration.GetSection(WatchdogOptions.SectionName))
    .ValidateDataAnnotations()
    .ValidateOnStart();
builder.Services.AddHostedService<StuckJobsWatchdog>();

builder.Services.AddSingleton<INpgsqlConnectionFactory, NpgsqlConnectionFactory>();
builder.Services.AddSingleton(_ => Channel.CreateUnbounded<StatusUpdate>());
builder.Services.AddSingleton(sp => sp.GetRequiredService<Channel<StatusUpdate>>().Writer);
builder.Services.AddSingleton(sp => sp.GetRequiredService<Channel<StatusUpdate>>().Reader);
builder.Services.AddHostedService<JobStatusBroadcastConsumer>();

builder.Services.AddOptions<MinioOptions>()
    .Bind(builder.Configuration.GetSection(MinioOptions.SectionName))
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddSingleton<IMinioClient>(sp =>
{
    var opts = sp.GetRequiredService<IOptions<MinioOptions>>().Value;
    return new MinioClient()
        .WithEndpoint(opts.Endpoint)
        .WithCredentials(opts.AccessKey, opts.SecretKey)
        .WithSSL(opts.UseSSL)
        .Build();
});
builder.Services.AddSingleton<IBucketOperations>(sp => (IBucketOperations)sp.GetRequiredService<IMinioClient>());
builder.Services.AddSingleton<IObjectOperations>(sp => (IObjectOperations)sp.GetRequiredService<IMinioClient>());

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

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

app.UseSwagger();
app.UseSwaggerUI(c => c.RoutePrefix = string.Empty);

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
        TicketNumber = request.TicketNumber,
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

app.MapGet("/jobs", async (
    [FromQuery] string? status,
    [FromQuery] int? limit,
    [FromQuery] int? offset,
    [FromQuery] string? sort,
    JobDbContext db,
    CancellationToken ct) =>
{
    var effectiveLimit = limit is null or <= 0 ? 50 : limit.Value;
    var effectiveOffset = offset is null or < 0 ? 0 : offset.Value;

    var query = db.Jobs.AsQueryable();

    if (!string.IsNullOrEmpty(status))
    {
        var statuses = status.Split(',')
            .Select(s => Enum.TryParse<JobStatus>(s.Trim(), true, out var js) ? js : (JobStatus?)null)
            .Where(s => s.HasValue)
            .Select(s => s!.Value)
            .ToList();

        if (statuses.Count > 0)
            query = query.Where(j => statuses.Contains(j.Status));
    }

    var total = await query.CountAsync(ct);

    if (sort == "status")
    {
        query = query.OrderBy(j =>
            j.Status == JobStatus.Printing ? 0 :
            j.Status == JobStatus.Requeued ? 1 :
            j.Status == JobStatus.Queued ? 2 :
            j.Status == JobStatus.Error ? 3 : 4);
    }

    var jobs = await query
        .Skip(effectiveOffset)
        .Take(effectiveLimit)
        .Select(j => new JobListItemDto(
            j.Id,
            j.Status.ToString().ToLower(),
            j.Total,
            j.Printed,
            j.RetryCount,
            j.TicketNumber,
            j.CreatedAt,
            j.UpdatedAt))
        .ToListAsync(ct);

    return Results.Ok(new JobListResponse(jobs, total));
});

app.MapGet("/jobs/{jobId:guid}", async (Guid jobId, JobDbContext db, CancellationToken ct) =>
{
    var job = await db.Jobs
        .Include(j => j.Photos)
        .FirstOrDefaultAsync(j => j.Id == jobId, ct);

    if (job is null)
        return Results.NotFound();

    return Results.Ok(new JobDetailDto(
        job.Id,
        job.Status.ToString().ToLower(),
        job.Total,
        job.Printed,
        job.RetryCount,
        job.TicketNumber,
        job.CreatedAt,
        job.UpdatedAt,
        job.Photos.Select(p => new JobPhotoDto(p.PhotoStorageKey, p.Copies)).ToList()));
});

app.MapGet("/photos", async (
    [FromQuery] int? limit,
    [FromQuery] int? offset,
    IBucketOperations bucket,
    IObjectOperations objects,
    IOptions<MinioOptions> minioOptions,
    CancellationToken ct) =>
{
    var opts = minioOptions.Value;
    var effectiveLimit = limit is null or <= 0 ? 50 : limit.Value;
    var effectiveOffset = offset is null or < 0 ? 0 : offset.Value;

    var listArgs = new ListObjectsArgs()
        .WithBucket(opts.Bucket)
        .WithPrefix("low/")
        .WithRecursive(true);

    var allKeys = new List<string>();
    await foreach (var item in bucket.ListObjectsEnumAsync(listArgs, ct))
    {
        if (!item.IsDir)
            allKeys.Add(item.Key);
    }

    allKeys.Sort(StringComparer.Ordinal);
    var total = allKeys.Count;
    var page = allKeys.Skip(effectiveOffset).Take(effectiveLimit).ToList();

    var photos = await Task.WhenAll(page.Select(async key =>
    {
        var presignArgs = new PresignedGetObjectArgs()
            .WithBucket(opts.Bucket)
            .WithObject(key)
            .WithExpiry(3600);
        var url = await objects.PresignedGetObjectAsync(presignArgs);
        return new PhotoEntry(key, url);
    }));

    return Results.Ok(new PhotoListResponse([.. photos], total));
});

var sseOptions = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

app.MapGet("/jobs/{jobId:guid}/stream", async (
    Guid jobId,
    JobDbContext db,
    ChannelReader<StatusUpdate> reader,
    HttpResponse response,
    CancellationToken ct) =>
{
    var job = await db.Jobs.AsNoTracking().FirstOrDefaultAsync(j => j.Id == jobId, ct);
    if (job is null)
        return Results.NotFound();

    response.ContentType = "text/event-stream";
    response.Headers["Cache-Control"] = "no-cache";
    response.Headers["X-Accel-Buffering"] = "no";

    var initial = new StatusUpdate(
        jobId.ToString(),
        job.Status.ToString().ToLower(),
        job.Printed,
        job.Total,
        job.Error);

    await WriteSseEventAsync(response, initial, ct);

    if (initial.Status is "done" or "error")
        return Results.Empty;

    var jobIdStr = jobId.ToString();

    await foreach (var update in reader.ReadAllAsync(ct))
    {
        if (update.JobId != jobIdStr)
            continue;

        await WriteSseEventAsync(response, update, ct);

        if (update.Status is "done" or "error")
            break;
    }

    return Results.Empty;
});

app.Run();

async Task WriteSseEventAsync(HttpResponse response, StatusUpdate update, CancellationToken ct)
{
    await response.WriteAsync($"data: {JsonSerializer.Serialize(update, sseOptions)}\n\n", ct);
    await response.Body.FlushAsync(ct);
}

public partial class Program { }
