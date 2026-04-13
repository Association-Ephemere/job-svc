# job-svc

ASP.NET Core .NET 8 minimal API that manages print jobs: creation, status tracking via SSE, RabbitMQ messaging and photo listing from MinIO.

## Role in the architecture

```mermaid
flowchart LR
    frontend["Frontend"]
    jobsvc["job-svc\n(Minimal API)"]
    rabbit["RabbitMQ\nphotostand exchange"]
    crdb["CockroachDB\njobs / job_photos"]
    minio["MinIO\nphotos/low/"]
    printsvc["print-svc"]

    frontend -->|POST /jobs| jobsvc
    frontend -->|GET /jobs/stream SSE| jobsvc
    frontend -->|GET /photos| jobsvc
    jobsvc -->|publish print.jobs| rabbit
    jobsvc -->|read/write| crdb
    jobsvc -->|list + presign| minio
    rabbit -->|print.status| jobsvc
    rabbit --> printsvc
```

## Requirements

- .NET 8 SDK for local development
- Docker for running via container
- CockroachDB, RabbitMQ and MinIO accessible on the network

## Configuration

Create an `appsettings.local.json` file at the root (not committed), or use environment variables with `__` as section separator:

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information"
    }
  },
  "ConnectionStrings": {
    "CockroachDb": "Host=localhost;Port=26257;Database=photostand;Username=root;SSL Mode=Disable"
  },
  "RabbitMq": {
    "Uri": "amqp://guest:guest@localhost:5672"
  },
  "MinIO": {
    "Endpoint": "localhost:9000",
    "AccessKey": "minioadmin",
    "SecretKey": "minioadmin",
    "UseSSL": false,
    "Bucket": "photostand"
  }
}
```

## Run with Docker

```bash
docker pull ghcr.io/association-ephemere/job-svc:latest
```

```bash
docker run \
  -e ConnectionStrings__CockroachDb="Host=<host>;Port=26257;Database=photostand;Username=root;SSL Mode=Disable" \
  -e RabbitMq__Uri="amqp://guest:guest@<host>:5672" \
  -e MinIO__Endpoint="<host>:9000" \
  -e MinIO__AccessKey=<access-key> \
  -e MinIO__SecretKey=<secret-key> \
  -e MinIO__Bucket=photostand \
  -p 8080:8080 \
  ghcr.io/association-ephemere/job-svc:latest
```

## Run in development

```bash
dotnet run --project src/JobSvc
```

## Run tests

```bash
dotnet test
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)
