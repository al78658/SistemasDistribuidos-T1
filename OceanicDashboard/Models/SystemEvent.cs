using System.ComponentModel.DataAnnotations;

namespace OceanicDashboard.Models
{
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
