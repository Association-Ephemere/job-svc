using System.ComponentModel.DataAnnotations;

namespace JobSvc.Data;

public class MinioOptions
{
    public const string SectionName = "MinIO";

    [Required]
    public string Endpoint { get; set; } = string.Empty;

    [Required]
    public string AccessKey { get; set; } = string.Empty;

    [Required]
    public string SecretKey { get; set; } = string.Empty;

    public bool UseSSL { get; set; } = false;

    [Required]
    public string Bucket { get; set; } = string.Empty;
}
