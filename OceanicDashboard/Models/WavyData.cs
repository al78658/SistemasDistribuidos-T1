using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace OceanicDashboard.Models
{
    public class WavyData
    {
        [Key]
        public int Id { get; set; }
        
        [Required]
        [MaxLength(50)]
        public string WavyId { get; set; } = string.Empty;
        
        [Required]
        [MaxLength(50)]
        public string SensorType { get; set; } = string.Empty;
        
        [Required]
        public DateTime Timestamp { get; set; }
        
        public double? Hs { get; set; }        // Altura significativa das ondas
        public double? Hmax { get; set; }      // Altura máxima das ondas
        public double? Tz { get; set; }        // Período médio
        public double? Tp { get; set; }        // Período de pico
        public double? PeakDirection { get; set; } // Direção de pico
        public double? SST { get; set; }       // Temperatura da superfície do mar
        
        [Required]
        public string RawData { get; set; } = string.Empty;
        
        [MaxLength(100)]
        public string PreprocessingApplied { get; set; } = string.Empty;
        
        public DateTime ReceivedAt { get; set; } = DateTime.UtcNow;
        
        [MaxLength(50)]
        public string ServerInstance { get; set; } = string.Empty;
    }
}
