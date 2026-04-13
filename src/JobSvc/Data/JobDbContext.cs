using JobSvc.Models;
using Microsoft.EntityFrameworkCore;

namespace JobSvc.Data;

public class JobDbContext : DbContext
{
    public JobDbContext(DbContextOptions<JobDbContext> options) : base(options)
    {
    }

    public DbSet<Job> Jobs => Set<Job>();
    public DbSet<JobPhoto> JobPhotos => Set<JobPhoto>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Job>(entity =>
        {
            entity.Property(e => e.Id).HasDefaultValueSql("gen_random_uuid()");
            entity.Property(e => e.Status)
                .HasConversion(
                    v => v.ToString().ToLower(),
                    v => Enum.Parse<JobStatus>(v, true))
                .HasDefaultValueSql("'queued'");
            entity.Property(e => e.Printed).HasDefaultValueSql("0");
            entity.Property(e => e.RetryCount).HasDefaultValueSql("0");
            entity.Property(e => e.CreatedAt).HasDefaultValueSql("now()");
            entity.Property(e => e.UpdatedAt).HasDefaultValueSql("now()");
        });

        modelBuilder.Entity<JobPhoto>(entity =>
        {
            entity.Property(e => e.Id).HasDefaultValueSql("gen_random_uuid()");
        });
    }
}
