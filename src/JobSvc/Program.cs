using JobSvc.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

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

builder.Services.AddSingleton<IRabbitMqConnectionManager, RabbitMqConnectionManager>();

var app = builder.Build();

// Ensure database is migrated on startup with retries (matching ingest-svc's robustness)
using (var scope = app.Services.CreateScope())
{
    var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();
    var db = scope.ServiceProvider.GetRequiredService<JobDbContext>();
    
    var retryCount = 0;
    const int maxRetries = 10;
    var delay = TimeSpan.FromSeconds(2);

    while (true)
    {
        try
        {
            logger.LogInformation("Applying database migrations (attempt {Attempt}/{MaxRetries})...", retryCount + 1, maxRetries);
            await db.Database.MigrateAsync();
            logger.LogInformation("Database migrations applied successfully.");
            break;
        }
        catch (Exception ex) when (retryCount < maxRetries)
        {
            retryCount++;
            logger.LogWarning(ex, "Database migration attempt {Attempt} failed. Retrying in {Delay}...", retryCount, delay);
            await Task.Delay(delay);
            delay = delay * 2; // Exponential backoff
        }
    }
}

app.MapGet("/health", () => Results.Ok());

app.Run();
