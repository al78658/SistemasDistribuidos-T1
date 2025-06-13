using Microsoft.EntityFrameworkCore;
using OceanicDashboard.Data;
using OceanicDashboard.Models;

namespace OceanicDashboard.Services
{
    public class DashboardService : IDashboardService
    {
        private readonly OceanicDataContext _context;
        
        public DashboardService(OceanicDataContext context)
        {
            _context = context;
        }
        
        public async Task<List<WavyData>> GetWavyDataAsync(int limit)
        {
            return await _context.WavyData
                .OrderByDescending(w => w.ReceivedAt)
                .Take(limit)
                .ToListAsync();
        }
        
        public async Task<List<WavyData>> GetWavyDataAsync(string? wavyId = null, DateTime? fromDate = null, DateTime? toDate = null, int limit = 100)
        {
            var query = _context.WavyData.AsQueryable();
            
            if (!string.IsNullOrEmpty(wavyId))
            {
                query = query.Where(w => w.WavyId == wavyId);
            }
            
            if (fromDate.HasValue)
            {
                query = query.Where(w => w.Timestamp >= fromDate.Value);
            }
            
            if (toDate.HasValue)
            {
                query = query.Where(w => w.Timestamp <= toDate.Value);
            }
            
            return await query
                .OrderByDescending(w => w.ReceivedAt)
                .Take(limit)
                .ToListAsync();
        }
          public async Task<List<AnalysisResult>> GetAnalysisResultsAsync(int limit)
        {
            return await _context.AnalysisResults
                .OrderByDescending(a => a.AnalysisTimestamp)
                .Take(limit)
                .ToListAsync();
        }
        
        public async Task<List<AnalysisResult>> GetAnalysisResultsAsync(string? wavyId = null, DateTime? fromDate = null, int limit = 100)
        {
            var query = _context.AnalysisResults.AsQueryable();
            
            if (!string.IsNullOrEmpty(wavyId))
            {
                query = query.Where(a => a.WavyId == wavyId);
            }
            
            if (fromDate.HasValue)
            {
                query = query.Where(a => a.AnalysisTimestamp >= fromDate.Value);
            }
            
            return await query
                .OrderByDescending(a => a.AnalysisTimestamp)
                .Take(limit)
                .ToListAsync();
        }
        
        public async Task<WavyData?> GetWavyDataByIdAsync(int id)
        {
            return await _context.WavyData.FindAsync(id);
        }
        
        public async Task<AnalysisResult?> GetAnalysisResultByIdAsync(int id)
        {
            return await _context.AnalysisResults.FindAsync(id);
        }
        
        public async Task<List<string>> GetDistinctWavyIdsAsync()
        {
            return await _context.WavyData
                .Select(w => w.WavyId)
                .Distinct()
                .OrderBy(id => id)
                .ToListAsync();
        }
        
        public async Task<Dictionary<string, object>> GetDashboardStatsAsync()
        {
            var stats = new Dictionary<string, object>();
            
            var totalRecords = await _context.WavyData.CountAsync();
            var totalWavys = await _context.WavyData.Select(w => w.WavyId).Distinct().CountAsync();
            var totalAnalyses = await _context.AnalysisResults.CountAsync();
            var lastDataReceived = await _context.WavyData
                .OrderByDescending(w => w.ReceivedAt)
                .Select(w => w.ReceivedAt)
                .FirstOrDefaultAsync();
              var recentAnalyses = await _context.AnalysisResults
                .Where(a => a.AnalysisTimestamp >= DateTime.UtcNow.AddDays(-7))
                .CountAsync();
            
            stats["TotalRecords"] = totalRecords;
            stats["TotalWavys"] = totalWavys;
            stats["TotalAnalyses"] = totalAnalyses;
            stats["LastDataReceived"] = lastDataReceived;
            stats["RecentAnalyses"] = recentAnalyses;
            
            return stats;
        }
    }
}
