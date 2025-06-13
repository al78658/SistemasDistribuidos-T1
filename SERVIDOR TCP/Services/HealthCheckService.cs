using Microsoft.Extensions.Logging;
using System.Net.NetworkInformation;

namespace SERVIDOR_TCP.Services
{
    public class HealthCheckService
    {
        private readonly ILogger<HealthCheckService> _logger;
        private readonly ConfigurationService _configService;
        
        public HealthCheckService()
        {
            using var loggerFactory = LoggerFactory.Create(builder =>
                builder.AddConsole());
            _logger = loggerFactory.CreateLogger<HealthCheckService>();
            _configService = new ConfigurationService();
        }
        
        public async Task<HealthStatus> CheckSystemHealthAsync()
        {
            var healthStatus = new HealthStatus
            {
                Timestamp = DateTime.UtcNow,
                OverallStatus = "Healthy"
            };
            
            // Verificar base de dados
            healthStatus.DatabaseStatus = await CheckDatabaseAsync();
              // Verificar serviços RPC
            healthStatus.PreProcessingServiceStatus = await CheckServiceAsync("PreProcessingService");
            healthStatus.AnalysisServiceStatus = await CheckServiceAsync("AnalysisService");
            
            // Verificar RabbitMQ
            healthStatus.RabbitMQStatus = await CheckRabbitMQAsync();            
            // Verificar portas TCP
            healthStatus.TcpServersStatus = await CheckTcpServersAsync();
            
            // Determinar status geral
            var allStatuses = new[]
            {
                healthStatus.DatabaseStatus,
                healthStatus.PreProcessingServiceStatus,
                healthStatus.AnalysisServiceStatus,
                healthStatus.RabbitMQStatus
            };
            
            if (allStatuses.Any(s => s == "Down"))
                healthStatus.OverallStatus = "Critical";
            else if (allStatuses.Any(s => s == "Degraded"))
                healthStatus.OverallStatus = "Degraded";
            
            return healthStatus;
        }
        
        private async Task<string> CheckDatabaseAsync()
        {
            try
            {
                using var dbService = new DatabaseService();
                // Tentar uma operação simples na BD
                await dbService.GetWavyDataAsync(limit: 1);
                return "Healthy";
            }
            catch (Exception ex)
            {
                _logger.LogError($"Erro ao verificar base de dados: {ex.Message}");
                return "Down";
            }
        }
        
        private async Task<string> CheckServiceAsync(string serviceName)
        {
            try
            {
                var config = _configService.GetServiceConfig(serviceName);
                var uri = new Uri(config.Url);
                
                using var ping = new Ping();
                var reply = await ping.SendPingAsync(uri.Host, 5000);
                
                if (reply.Status == IPStatus.Success)
                {
                    // Verificar se a porta está aberta
                    using var tcpClient = new System.Net.Sockets.TcpClient();
                    var connectTask = tcpClient.ConnectAsync(uri.Host, uri.Port);
                    var timeoutTask = Task.Delay(3000);
                    
                    if (await Task.WhenAny(connectTask, timeoutTask) == connectTask)
                    {
                        return "Healthy";
                    }
                }
                
                return "Degraded";
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"Erro ao verificar serviço {serviceName}: {ex.Message}");
                return "Down";
            }
        }
        
        private async Task<string> CheckRabbitMQAsync()
        {
            try
            {
                var config = _configService.GetRabbitMQConfig();
                
                using var ping = new Ping();
                var reply = await ping.SendPingAsync(config.HostName, 3000);
                
                return reply.Status == IPStatus.Success ? "Healthy" : "Down";
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"Erro ao verificar RabbitMQ: {ex.Message}");
                return "Down";
            }
        }
        
        private async Task<Dictionary<string, string>> CheckTcpServersAsync()
        {
            var serverConfigs = _configService.GetTcpServerConfigs();
            var results = new Dictionary<string, string>();
            
            foreach (var serverConfig in serverConfigs)
            {
                try
                {
                    using var tcpClient = new System.Net.Sockets.TcpClient();
                    var connectTask = tcpClient.ConnectAsync("localhost", serverConfig.Port);
                    var timeoutTask = Task.Delay(2000);
                    
                    if (await Task.WhenAny(connectTask, timeoutTask) == connectTask)
                    {
                        results[serverConfig.Name] = "Healthy";
                    }
                    else
                    {
                        results[serverConfig.Name] = "Down";
                    }
                }
                catch
                {
                    results[serverConfig.Name] = "Down";
                }
            }
            
            return results;
        }
        
        public async Task LogSystemMetricsAsync()
        {
            try
            {
                var healthStatus = await CheckSystemHealthAsync();
                  _logger.LogInformation($"[HEALTH] Status Geral: {healthStatus.OverallStatus}");
                _logger.LogInformation($"[HEALTH] Base de Dados: {healthStatus.DatabaseStatus}");
                _logger.LogInformation($"[HEALTH] Pré-processamento: {healthStatus.PreProcessingServiceStatus}");
                _logger.LogInformation($"[HEALTH] Análise: {healthStatus.AnalysisServiceStatus}");
                _logger.LogInformation($"[HEALTH] RabbitMQ: {healthStatus.RabbitMQStatus}");
                
                // Log uso de memória
                var process = System.Diagnostics.Process.GetCurrentProcess();
                var memoryMB = process.WorkingSet64 / (1024 * 1024);
                _logger.LogInformation($"[METRICS] Memória em uso: {memoryMB} MB");
                
                // Log de threads ativas
                var threadCount = process.Threads.Count;
                _logger.LogInformation($"[METRICS] Threads ativas: {threadCount}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Erro ao registrar métricas do sistema: {ex.Message}");
            }
        }
    }
    
    public class HealthStatus
    {
        public DateTime Timestamp { get; set; }        public string OverallStatus { get; set; } = "Unknown";
        public string DatabaseStatus { get; set; } = "Unknown";
        public string PreProcessingServiceStatus { get; set; } = "Unknown";
        public string AnalysisServiceStatus { get; set; } = "Unknown";
        public string RabbitMQStatus { get; set; } = "Unknown";
        public Dictionary<string, string> TcpServersStatus { get; set; } = new();
    }
}
