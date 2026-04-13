using System.ComponentModel.DataAnnotations;

namespace JobSvc.Data;

public class DatabaseOptions
{
    public const string SectionName = "ConnectionStrings";

    [Required(AllowEmptyStrings = false)]
    public string CockroachDb { get; set; } = string.Empty;
}
