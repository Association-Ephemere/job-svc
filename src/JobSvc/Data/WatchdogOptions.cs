using System.ComponentModel.DataAnnotations;

namespace JobSvc.Data;

public class WatchdogOptions
{
    public const string SectionName = "Watchdog";

    [Range(1, int.MaxValue)]
    public int IntervalMinutes { get; set; } = 5;

    [Range(1, int.MaxValue)]
    public int StaleThresholdMinutes { get; set; } = 5;

    [Range(1, int.MaxValue)]
    public int MaxRetries { get; set; } = 3;
}
