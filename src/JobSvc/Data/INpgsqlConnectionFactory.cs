using Microsoft.Extensions.Options;
using Npgsql;

namespace JobSvc.Data;

public interface INpgsqlConnectionFactory
{
    NpgsqlConnection Create();
}

public class NpgsqlConnectionFactory : INpgsqlConnectionFactory
{
    private readonly string _connectionString;

    public NpgsqlConnectionFactory(IOptions<DatabaseOptions> options)
        => _connectionString = options.Value.CockroachDb;

    public NpgsqlConnection Create() => new(_connectionString);
}
