using Microsoft.EntityFrameworkCore;
using OceanicDashboard.Models;

namespace OceanicDashboard.Data
{
    public class OceanicDataContext : DbContext
    {
        public DbSet<WavyData> WavyData { get; set; }
        public DbSet<AnalysisResult> AnalysisResults { get; set; }
        public DbSet<SystemEvent> SystemEvents { get; set; }
        
        public OceanicDataContext(DbContextOptions<OceanicDataContext> options) : base(options)
        {
        }
        
        public OceanicDataContext() : base()
        {
        }
          protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                // Usar SQLite compartilhado entre SERVIDOR TCP e Dashboard
                var dbPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop), "SistemasDistribuidos-T1", "oceanic_data.db");
                optionsBuilder.UseSqlite($"Data Source={dbPath}");
            }
        }
        
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            
            // Configurações específicas das entidades
            modelBuilder.Entity<WavyData>(entity =>
            {
                entity.HasIndex(e => e.WavyId);
                entity.HasIndex(e => e.Timestamp);
                entity.HasIndex(e => e.SensorType);
                entity.HasIndex(e => new { e.WavyId, e.Timestamp });
            });
            
            modelBuilder.Entity<AnalysisResult>(entity =>
            {
                entity.HasIndex(e => e.WavyId);
                entity.HasIndex(e => e.AnalysisTimestamp);
                entity.HasIndex(e => new { e.WavyId, e.AnalysisTimestamp });
            });
            
            modelBuilder.Entity<SystemEvent>(entity =>
            {
                entity.HasIndex(e => e.Timestamp);
                entity.HasIndex(e => e.EventType);
                entity.HasIndex(e => e.Component);
                entity.HasIndex(e => e.WavyId);
            });
        }
    }
}
