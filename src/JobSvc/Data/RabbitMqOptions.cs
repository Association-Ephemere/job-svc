using System.ComponentModel.DataAnnotations;

namespace JobSvc.Data;

public class RabbitMqOptions
{
    public const string SectionName = "RabbitMq";

    [Required]
    public string Uri { get; set; } = string.Empty;
}
