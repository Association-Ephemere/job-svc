using RabbitMQ.Client;

namespace JobSvc.Data;

public interface IRabbitMqConnectionManager : IDisposable
{
    Task<IConnection> GetConnectionAsync(CancellationToken ct = default);
}
