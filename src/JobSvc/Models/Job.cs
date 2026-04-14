using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace JobSvc.Models;

public class Job
{
    [Key]
    public Guid Id { get; set; }

    [Required]
    public JobStatus Status { get; set; } = JobStatus.Queued;

    [Required]
    public int Total { get; set; }

    [Required]
    public int Printed { get; set; } = 0;

    public string? Error { get; set; }

    [Required]
    public int RetryCount { get; set; } = 0;

    [Required]
    public int TicketNumber { get; set; }

    [Required]
    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;

    [Required]
    public DateTimeOffset UpdatedAt { get; set; } = DateTimeOffset.UtcNow;

    public ICollection<JobPhoto> Photos { get; set; } = new List<JobPhoto>();
}
