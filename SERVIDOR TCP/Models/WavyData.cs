using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace SERVIDOR_TCP.Models
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
    
    public class AnalysisResult
    {
        [Key]
        public int Id { get; set; }
        
        [Required]
        [MaxLength(50)]
        public string WavyId { get; set; } = string.Empty;
        
        [Required]
        public DateTime AnalysisTimestamp { get; set; }
        
        public double Mean { get; set; }
        public double StandardDeviation { get; set; }
        
        [MaxLength(50)]
        public string Pattern { get; set; } = string.Empty;
        
        public int SampleCount { get; set; }
        
        [Required]
        public string AnalyzedData { get; set; } = string.Empty;
        
        [MaxLength(100)]
        public string AnalysisType { get; set; } = string.Empty;
        
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    }
    
    public class SystemEvent
    {
        [Key]
        public int Id { get; set; }
        
        [Required]
        public DateTime Timestamp { get; set; }
        
        [Required]
        [MaxLength(50)]
        public string EventType { get; set; } = string.Empty; // INFO, WARNING, ERROR
        
        [Required]
        [MaxLength(100)]
        public string Component { get; set; } = string.Empty; // SERVIDOR, AGREGADOR, etc.
        
        [Required]
        public string Message { get; set; } = string.Empty;
        
        [MaxLength(50)]
        public string WavyId { get; set; } = string.Empty;
        
        public string? AdditionalData { get; set; }
    }
}
