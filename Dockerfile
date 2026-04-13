FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY src/JobSvc/JobSvc.csproj src/JobSvc/
RUN dotnet restore src/JobSvc/JobSvc.csproj
COPY src/ src/
RUN dotnet publish src/JobSvc/JobSvc.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
USER app
ENTRYPOINT ["dotnet", "JobSvc.dll"]
