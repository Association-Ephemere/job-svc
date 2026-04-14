using System.ComponentModel.DataAnnotations;

namespace JobSvc.Models;

public record PhotoRequest(
    [Required] string PhotoStorageKey,
    [Required, Range(1, int.MaxValue)] int Copies
);

public record CreateJobRequest(
    [Required] List<PhotoRequest> Photos,
    [Required] int TicketNumber
);

public record JobResponse(
    Guid JobId,
    string Status,
    DateTimeOffset CreatedAt
);

public record JobListItemDto(
    Guid Id,
    string Status,
    int Total,
    int Printed,
    int RetryCount,
    int TicketNumber,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt
);

public record JobListResponse(
    List<JobListItemDto> Jobs,
    int Total
);

public record JobPhotoDto(
    string PhotoStorageKey,
    int Copies
);

public record JobDetailDto(
    Guid Id,
    string Status,
    int Total,
    int Printed,
    int RetryCount,
    int TicketNumber,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt,
    List<JobPhotoDto> Photos
);
