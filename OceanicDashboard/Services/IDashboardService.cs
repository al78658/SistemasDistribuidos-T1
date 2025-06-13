using OceanicDashboard.Models;

namespace OceanicDashboard.Services
{
    public interface IDashboardService
    {
        Task<List<WavyData>> GetWavyDataAsync(int limit);
        Task<List<WavyData>> GetWavyDataAsync(string? wavyId = null, DateTime? fromDate = null, DateTime? toDate = null, int limit = 100);
        Task<List<AnalysisResult>> GetAnalysisResultsAsync(int limit);
        Task<List<AnalysisResult>> GetAnalysisResultsAsync(string? wavyId = null, DateTime? fromDate = null, int limit = 100);
        Task<WavyData?> GetWavyDataByIdAsync(int id);
        Task<AnalysisResult?> GetAnalysisResultByIdAsync(int id);
        Task<List<string>> GetDistinctWavyIdsAsync();
        Task<Dictionary<string, object>> GetDashboardStatsAsync();
    }
}
