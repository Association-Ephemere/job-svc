using System.ComponentModel.DataAnnotations;

namespace JobSvc.Data;

public class DatabaseOptions
{
    public const string SectionName = "ConnectionStrings";

    [Required(AllowEmptyStrings = false)]
    public string Postgres { get; set; } = string.Empty;
}
