namespace JobSvc.Models;

public record PhotoEntry(string Key, string Url);

public record PhotoListResponse(List<PhotoEntry> Photos, int Total);
