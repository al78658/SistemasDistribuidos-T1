using System.ComponentModel.DataAnnotations;

namespace OceanicDashboard.Models
{
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
}
