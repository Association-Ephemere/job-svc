namespace JobSvc.Models;

public record PrintStatusMessage(
    Guid JobId,
    string Status,
    int Printed,
    int Total,
    string? Error
);
