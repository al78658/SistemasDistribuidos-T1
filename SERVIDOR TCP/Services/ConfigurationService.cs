using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace SERVIDOR_TCP.Services
{
    public class ConfigurationService
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<ConfigurationService> _logger;
          public ConfigurationService()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory());
            
            // Tentar adicionar appsettings.json se existir
            var appSettingsPath = Path.Combine(Directory.GetCurrentDirectory(), "appsettings.json");
            if (File.Exists(appSettingsPath))
            {
                builder.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
                Console.WriteLine("[CONFIG] ✓ appsettings.json carregado");
            }
            else
            {
                Console.WriteLine("[CONFIG] ⚠️ appsettings.json não encontrado, usando valores padrão");
                Console.WriteLine($"[CONFIG] Procurado em: {appSettingsPath}");
            }
            
            builder.AddEnvironmentVariables();
            _configuration = builder.Build();
            
            using var loggerFactory = LoggerFactory.Create(builder =>
                builder.AddConsole());
            _logger = loggerFactory.CreateLogger<ConfigurationService>();
        }
        
        public T GetSection<T>(string sectionName) where T : new()
        {
            var section = _configuration.GetSection(sectionName);
            var result = new T();
            section.Bind(result);
            return result;
        }
        
        public string GetConnectionString(string name = "DefaultConnection")
        {
            return _configuration.GetConnectionString(name) ?? "Data Source=oceanic_data.db";
        }
        
        public int GetRetryAttempts()
        {
            return _configuration.GetValue<int>("OceanicSystem:MaxRetryAttempts", 3);
        }
        
        public int GetRetryDelay()
        {
            return _configuration.GetValue<int>("OceanicSystem:RetryDelaySeconds", 2);
        }
          public int GetAnalysisVolumeThreshold()
        {
            var value = _configuration.GetValue<int>("OceanicSystem:AnalysisVolumeThreshold", 3);
            Console.WriteLine($"[CONFIG] Volume de análise: {value} (padrão: 3)");
            return value;
        }
        
        public ServiceConfig GetServiceConfig(string serviceName)
        {
            var section = _configuration.GetSection($"Services:{serviceName}");
            return new ServiceConfig
            {
                Url = section.GetValue<string>("Url") ?? "",
                Timeout = section.GetValue<int>("Timeout", 30),
                MaxConcurrentRequests = section.GetValue<int>("MaxConcurrentRequests", 5)
            };
        }
        
        public RabbitMQConfig GetRabbitMQConfig()
        {
            return GetSection<RabbitMQConfig>("RabbitMQ");
        }
        
        public List<TcpServerConfig> GetTcpServerConfigs()
        {
            var servers = new List<TcpServerConfig>();
            _configuration.GetSection("TCP:Servers").Bind(servers);
            return servers;
        }
    }
    
    public class ServiceConfig
    {
        public string Url { get; set; } = "";
        public int Timeout { get; set; } = 30;
        public int MaxConcurrentRequests { get; set; } = 5;
    }
    
    public class RabbitMQConfig
    {
        public string HostName { get; set; } = "localhost";
        public int Port { get; set; } = 5672;
        public string ExchangeName { get; set; } = "wavy_data";
        public int MaxRetryAttempts { get; set; } = 5;
        public int RetryDelaySeconds { get; set; } = 1;
    }
    
    public class TcpServerConfig
    {
        public int Port { get; set; }
        public string Name { get; set; } = "";
        public string LogFile { get; set; } = "";
    }
    
    public class OceanicSystemConfig
    {
        public int MaxRetryAttempts { get; set; } = 3;
        public int RetryDelaySeconds { get; set; } = 2;
        public int AnalysisVolumeThreshold { get; set; } = 5;
        public int DataRetentionDays { get; set; } = 30;
        public int BackupIntervalHours { get; set; } = 24;
    }
}
