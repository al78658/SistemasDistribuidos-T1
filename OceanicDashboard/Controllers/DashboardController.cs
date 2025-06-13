using Microsoft.AspNetCore.Mvc;
using OceanicDashboard.Models;
using OceanicDashboard.Data;
using OceanicDashboard.Services;
using Microsoft.EntityFrameworkCore;

namespace OceanicDashboard.Controllers
{    public class DashboardController : Controller
    {
        private readonly IDashboardService _dashboardService;
        
        public DashboardController(IDashboardService dashboardService)
        {
            _dashboardService = dashboardService;
        }
          public async Task<IActionResult> Index()
        {
            // Obter dados recentes para o dashboard
            var recentData = await _dashboardService.GetWavyDataAsync(limit: 10);
            var recentAnalyses = await _dashboardService.GetAnalysisResultsAsync(limit: 5);
            
            var viewModel = new DashboardViewModel
            {
                RecentData = recentData,
                RecentAnalyses = recentAnalyses,
                TotalWavys = recentData.Select(d => d.WavyId).Distinct().Count(),
                TotalRecords = recentData.Count(),
                LastUpdate = recentData.FirstOrDefault()?.ReceivedAt ?? DateTime.Now
            };
            
            return View(viewModel);
        }
          public async Task<IActionResult> Data(string? wavyId, DateTime? fromDate, DateTime? toDate, int page = 1)
        {
            const int pageSize = 20;
            
            var data = await _dashboardService.GetWavyDataAsync(
                wavyId, 
                fromDate, 
                toDate, 
                pageSize * page
            );
            
            var viewModel = new DataViewModel
            {
                Data = data.Skip((page - 1) * pageSize).Take(pageSize).ToList(),
                WavyId = wavyId,
                FromDate = fromDate,
                ToDate = toDate,
                CurrentPage = page,
                PageSize = pageSize,
                TotalRecords = data.Count
            };
            
            return View(viewModel);
        }
          public async Task<IActionResult> Analyses(string? wavyId, DateTime? fromDate, int page = 1)
        {
            const int pageSize = 15;
            
            var analyses = await _dashboardService.GetAnalysisResultsAsync(
                wavyId, 
                fromDate, 
                pageSize * page
            );
            
            var viewModel = new AnalysesViewModel
            {
                Analyses = analyses.Skip((page - 1) * pageSize).Take(pageSize).ToList(),
                WavyId = wavyId,
                FromDate = fromDate,
                CurrentPage = page,
                PageSize = pageSize,
                TotalRecords = analyses.Count
            };
            
            return View(viewModel);
        }        [HttpGet]
        public async Task<IActionResult> GetChartData(string? wavyId)
        {
            try
            {
                Console.WriteLine($"[DASHBOARD] GetChartData chamado para wavyId: {wavyId ?? "todos"}");
                
                // Buscar todos os dados primeiro, sem filtro de tempo
                var allData = await _dashboardService.GetWavyDataAsync(wavyId, null, null, 100);
                Console.WriteLine($"[DASHBOARD] Total de dados na BD: {allData.Count}");
                
                // Se não houver dados, buscar dados das últimas 24h
                if (!allData.Any())
                {
                    allData = await _dashboardService.GetWavyDataAsync(wavyId, DateTime.Now.AddHours(-24), null, 50);
                    Console.WriteLine($"[DASHBOARD] Dados últimas 24h: {allData.Count}");
                }
                
                if (allData.Any())
                {
                    var first = allData.First();
                    var last = allData.Last();
                    Console.WriteLine($"[DASHBOARD] Primeiro registro: WavyId={first.WavyId}, Timestamp={first.Timestamp}, Hs={first.Hs}, Hmax={first.Hmax}, SST={first.SST}");
                    Console.WriteLine($"[DASHBOARD] Último registro: WavyId={last.WavyId}, Timestamp={last.Timestamp}, Hs={last.Hs}, Hmax={last.Hmax}, SST={last.SST}");
                    
                    // Contar registros com valores válidos
                    var withHs = allData.Count(d => d.Hs.HasValue);
                    var withHmax = allData.Count(d => d.Hmax.HasValue);
                    var withSST = allData.Count(d => d.SST.HasValue);
                    var withTp = allData.Count(d => d.Tp.HasValue);
                    var withTz = allData.Count(d => d.Tz.HasValue);
                    
                    Console.WriteLine($"[DASHBOARD] Registros com valores - Hs: {withHs}, Hmax: {withHmax}, SST: {withSST}, Tp: {withTp}, Tz: {withTz}");
                }
                  // Análise dos dados antes de retornar
                var hasRealValues = allData.Any(d => 
                    d.Hs.HasValue || d.Hmax.HasValue || d.SST.HasValue || 
                    d.Tp.HasValue || d.Tz.HasValue);
                
                Console.WriteLine($"[DASHBOARD] Análise de dados reais: {hasRealValues}");
                
                if (!hasRealValues && allData.Any())
                {
                    Console.WriteLine("[DASHBOARD] ⚠️ DADOS SEM VALORES: Todos os registros têm valores nulos");
                    Console.WriteLine("[DASHBOARD] 💡 SOLUÇÃO: Execute no Servidor TCP -> Opção 2 (Reprocessar dados)");
                }
                
                // Retornar todos os dados para que o frontend decida o que exibir
                var chartData = allData
                    .OrderBy(d => d.Timestamp)
                    .Select(d => new 
                    {
                        timestamp = d.Timestamp.ToString("yyyy-MM-dd HH:mm:ss"),
                        hs = d.Hs,
                        hmax = d.Hmax,
                        tz = d.Tz,
                        tp = d.Tp,
                        sst = d.SST,
                        wavyId = d.WavyId,
                        rawData = d.RawData?.Length > 100 ? d.RawData.Substring(0, 100) + "..." : d.RawData
                    })
                    .ToList();
                
                Console.WriteLine($"[DASHBOARD] Retornando {chartData.Count} registros para o gráfico");
                
                if (chartData.Any())
                {
                    var sample = chartData.Take(3).ToList();
                    foreach (var item in sample)
                    {
                        Console.WriteLine($"[DASHBOARD] Amostra: {item.wavyId} - Hs:{item.hs}, Hmax:{item.hmax}, SST:{item.sst}");
                    }
                }
                
                return Json(chartData);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DASHBOARD] ERRO em GetChartData: {ex.Message}");
                Console.WriteLine($"[DASHBOARD] Stack trace: {ex.StackTrace}");
                return Json(new { error = ex.Message, details = "Verifique os logs do servidor para mais detalhes" });
            }
        }
    }
    
    public class DashboardViewModel
    {
        public List<WavyData> RecentData { get; set; } = new();
        public List<AnalysisResult> RecentAnalyses { get; set; } = new();
        public int TotalWavys { get; set; }
        public int TotalRecords { get; set; }
        public DateTime LastUpdate { get; set; }
    }
    
    public class DataViewModel
    {
        public List<WavyData> Data { get; set; } = new();
        public string? WavyId { get; set; }
        public DateTime? FromDate { get; set; }
        public DateTime? ToDate { get; set; }
        public int CurrentPage { get; set; }
        public int PageSize { get; set; }
        public int TotalRecords { get; set; }
    }
    
    public class AnalysesViewModel
    {
        public List<AnalysisResult> Analyses { get; set; } = new();
        public string? WavyId { get; set; }
        public DateTime? FromDate { get; set; }
        public int CurrentPage { get; set; }
        public int PageSize { get; set; }
        public int TotalRecords { get; set; }
    }
}
