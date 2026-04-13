using System.ComponentModel.DataAnnotations;

namespace JobSvc.Models;

public record PhotoRequest(
    [Required] string PhotoStorageKey,
    [Required, Range(1, int.MaxValue)] int Copies
);

public record CreateJobRequest(
    [Required] List<PhotoRequest> Photos
);

public record JobResponse(
    Guid JobId,
    string Status,
    DateTimeOffset CreatedAt
);
