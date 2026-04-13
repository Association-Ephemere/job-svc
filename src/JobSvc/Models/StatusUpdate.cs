namespace JobSvc.Models;

public record StatusUpdate(
    string JobId,
    string Status,
    int Printed,
    int Total,
    string? Error
);
