using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace JobSvc.Models;

public class JobPhoto
{
    [Key]
    public Guid Id { get; set; }

    [Required]
    public Guid JobId { get; set; }

    [ForeignKey(nameof(JobId))]
    public Job Job { get; set; } = null!;

    [Required]
    public string PhotoStorageKey { get; set; } = string.Empty;

    [Required]
    public int Copies { get; set; }
}
