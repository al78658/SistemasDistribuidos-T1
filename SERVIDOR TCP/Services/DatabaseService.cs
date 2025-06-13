using SERVIDOR_TCP.Data;
using SERVIDOR_TCP.Models;
using Microsoft.EntityFrameworkCore;
using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace SERVIDOR_TCP.Services
{
    public class DatabaseService : IDisposable
    {
        private readonly OceanicDataContext _context;
        private bool _disposed = false;
        
        public DatabaseService()
        {
            _context = new OceanicDataContext();
            EnsureDatabaseCreated();
        }
        
        public DatabaseService(OceanicDataContext context)
        {
            _context = context;
            EnsureDatabaseCreated();
        }
          private void EnsureDatabaseCreated()
        {
            try
            {
                _context.Database.EnsureCreated();
                Console.WriteLine("[BD] ✓ Base de dados inicializada com sucesso");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[BD] ❌ Erro ao inicializar base de dados: {ex.Message}");
                Console.WriteLine($"[BD] Stack trace: {ex.StackTrace}");
                throw; // Re-throw para que o erro seja tratado no nível superior
            }
        }
          public async Task<bool> SaveWavyDataAsync(string wavyId, string rawData, string preprocessingApplied = "")
        {
            try
            {
                var parsedData = ParseWavyData(rawData);
                
                var wavyData = new WavyData
                {
                    WavyId = wavyId,
                    SensorType = "oceanic",
                    Timestamp = DateTime.UtcNow,
                    RawData = rawData,
                    PreprocessingApplied = preprocessingApplied,
                    ServerInstance = Environment.MachineName,
                    ReceivedAt = DateTime.UtcNow,
                    Hs = parsedData.ContainsKey("Hs") ? parsedData["Hs"] : null,
                    Hmax = parsedData.ContainsKey("Hmax") ? parsedData["Hmax"] : null,
                    Tz = parsedData.ContainsKey("Tz") ? parsedData["Tz"] : null,
                    Tp = parsedData.ContainsKey("Tp") ? parsedData["Tp"] : null,
                    PeakDirection = parsedData.ContainsKey("Peak Direction") ? parsedData["Peak Direction"] : 
                                   (parsedData.ContainsKey("Direction") ? parsedData["Direction"] : null),
                    SST = parsedData.ContainsKey("SST") ? parsedData["SST"] : null
                };
                
                _context.WavyData.Add(wavyData);
                var result = await _context.SaveChangesAsync();
                
                Console.WriteLine($"[BD] ✓ Dados salvos na BD para WAVY {wavyId} - Registros salvos: {result}");
                LogSystemEvent("INFO", "SERVIDOR", $"Dados salvos para WAVY {wavyId}", wavyId);
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[BD] ❌ Erro ao salvar dados da WAVY {wavyId}: {ex.Message}");
                Console.WriteLine($"[BD] Stack trace: {ex.StackTrace}");
                LogSystemEvent("ERROR", "SERVIDOR", $"Erro ao salvar dados: {ex.Message}", wavyId);
                return false;
            }
        }
        
        public async Task<bool> SaveAnalysisResultAsync(string wavyId, double mean, double stddev, string pattern, string analyzedData, int sampleCount)
        {
            try
            {
                var analysisResult = new AnalysisResult
                {
                    WavyId = wavyId,
                    AnalysisTimestamp = DateTime.UtcNow,
                    Mean = mean,
                    StandardDeviation = stddev,
                    Pattern = pattern,
                    AnalyzedData = analyzedData,
                    SampleCount = sampleCount,
                    AnalysisType = "statistical"
                };
                
                _context.AnalysisResults.Add(analysisResult);
                await _context.SaveChangesAsync();
                
                LogSystemEvent("INFO", "ANÁLISE", $"Análise concluída para WAVY {wavyId}: Média={mean:F2}, Padrão={pattern}", wavyId);
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[BD] Erro ao salvar resultado de análise para WAVY {wavyId}: {ex.Message}");
                LogSystemEvent("ERROR", "ANÁLISE", $"Erro ao salvar análise: {ex.Message}", wavyId);
                return false;
            }
        }
        
        public async Task<List<WavyData>> GetWavyDataAsync(string? wavyId = null, DateTime? fromDate = null, DateTime? toDate = null, int limit = 100)
        {
            try
            {
                var query = _context.WavyData.AsQueryable();
                
                if (!string.IsNullOrEmpty(wavyId))
                    query = query.Where(w => w.WavyId == wavyId);
                
                if (fromDate.HasValue)
                    query = query.Where(w => w.Timestamp >= fromDate.Value);
                
                if (toDate.HasValue)
                    query = query.Where(w => w.Timestamp <= toDate.Value);
                
                return await query
                    .OrderByDescending(w => w.Timestamp)
                    .Take(limit)
                    .ToListAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[BD] Erro ao consultar dados: {ex.Message}");
                return new List<WavyData>();
            }
        }
        
        public async Task<List<AnalysisResult>> GetAnalysisResultsAsync(string? wavyId = null, DateTime? fromDate = null, int limit = 50)
        {
            try
            {
                var query = _context.AnalysisResults.AsQueryable();
                
                if (!string.IsNullOrEmpty(wavyId))
                    query = query.Where(a => a.WavyId == wavyId);
                
                if (fromDate.HasValue)
                    query = query.Where(a => a.AnalysisTimestamp >= fromDate.Value);
                
                return await query
                    .OrderByDescending(a => a.AnalysisTimestamp)
                    .Take(limit)
                    .ToListAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[BD] Erro ao consultar análises: {ex.Message}");
                return new List<AnalysisResult>();
            }
        }
        
        public void LogSystemEvent(string eventType, string component, string message, string? wavyId = null, string? additionalData = null)
        {
            try
            {
                var systemEvent = new SystemEvent
                {
                    Timestamp = DateTime.UtcNow,
                    EventType = eventType,
                    Component = component,
                    Message = message,
                    WavyId = wavyId ?? string.Empty,
                    AdditionalData = additionalData
                };
                
                _context.SystemEvents.Add(systemEvent);
                _context.SaveChanges(); // Síncrono para logs
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[BD] Erro ao registrar evento: {ex.Message}");
            }
        }        private Dictionary<string, double> ParseWavyData(string rawData)
        {
            var result = new Dictionary<string, double>();
            
            try
            {
                Console.WriteLine($"[BD-PARSE] Tentando fazer parse de: {rawData}");
                
                // Tentar parse JSON primeiro
                if (rawData.Trim().StartsWith("{") || rawData.Trim().StartsWith("["))
                {
                    var jsonDoc = JsonDocument.Parse(rawData);
                    
                    if (jsonDoc.RootElement.ValueKind == JsonValueKind.Object)
                    {
                        ParseJsonObject(jsonDoc.RootElement, result);
                    }
                    else if (jsonDoc.RootElement.ValueKind == JsonValueKind.Array)
                    {
                        // Se for array, processa cada objeto
                        foreach (var element in jsonDoc.RootElement.EnumerateArray())
                        {
                            if (element.ValueKind == JsonValueKind.Object)
                            {
                                ParseJsonObject(element, result);
                            }
                        }
                    }
                }
                else
                {
                    // Parse texto simples (formato: "campo=valor campo2=valor2")
                    var parts = rawData.Split(new char[] { ' ', ',', ';', '\n', '\t' }, StringSplitOptions.RemoveEmptyEntries);
                    foreach (var part in parts)
                    {
                        var keyValue = part.Split('=');
                        if (keyValue.Length == 2 && double.TryParse(keyValue[1], System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double value))
                        {
                            result[keyValue[0]] = value;
                        }
                    }
                }
                
                // Se não conseguiu extrair nada, tentar padrões mais específicos
                if (result.Count == 0)
                {
                    Console.WriteLine("[BD-PARSE] Nenhum valor extraído, tentando padrões alternativos...");
                    
                    // Tentar extrair valores usando regex ou padrões conhecidos
                    var patterns = new Dictionary<string, string>
                    {
                        { "Hs", @"Hs[:\s]*([0-9]+\.?[0-9]*)" },
                        { "Hmax", @"Hmax[:\s]*([0-9]+\.?[0-9]*)" },
                        { "SST", @"SST[:\s]*([0-9]+\.?[0-9]*)" },
                        { "Tp", @"Tp[:\s]*([0-9]+\.?[0-9]*)" },
                        { "Tz", @"Tz[:\s]*([0-9]+\.?[0-9]*)" }
                    };
                    
                    foreach (var pattern in patterns)
                    {
                        var match = System.Text.RegularExpressions.Regex.Match(rawData, pattern.Value);
                        if (match.Success && double.TryParse(match.Groups[1].Value, out double value))
                        {
                            result[pattern.Key] = value;
                        }
                    }
                }
                
                Console.WriteLine($"[BD-PARSE] Valores extraídos: {string.Join(", ", result.Select(kv => $"{kv.Key}={kv.Value}"))}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[BD] Erro ao fazer parse dos dados: {ex.Message}");
                Console.WriteLine($"[BD-PARSE] Dados que causaram erro: {rawData}");
            }
            
            return result;
        }          private void ParseJsonObject(JsonElement element, Dictionary<string, double> result)
        {
            foreach (var prop in element.EnumerateObject())
            {
                try
                {
                    if (prop.Value.ValueKind == JsonValueKind.Number)
                    {
                        result[prop.Name] = prop.Value.GetDouble();
                        Console.WriteLine($"[BD-PARSE] Número direto: {prop.Name} = {prop.Value.GetDouble()}");
                    }
                    else if (prop.Value.ValueKind == JsonValueKind.String)
                    {
                        var stringValue = prop.Value.GetString();
                        if (!string.IsNullOrEmpty(stringValue) && double.TryParse(stringValue, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double value))
                        {
                            result[prop.Name] = value;
                            Console.WriteLine($"[BD-PARSE] String convertida: {prop.Name} = {value}");
                        }
                    }
                    else if (prop.Value.ValueKind == JsonValueKind.Array && prop.Name == "records")
                    {
                        Console.WriteLine($"[BD-PARSE] Processando array 'records' com {prop.Value.GetArrayLength()} elementos...");
                        
                        // Esta é a estrutura real dos dados que chegam do AGREGADOR:
                        // "records": [{"data":"1.407","timestamp":"..."}, {"data":"2.5","timestamp":"..."}, ...]
                        // OU
                        // "records": [{"data":"1.407","sensor_type":"Hs"}, {"data":"2.5","sensor_type":"Hmax"}, ...]
                        
                        foreach (var record in prop.Value.EnumerateArray())
                        {
                            if (record.ValueKind == JsonValueKind.Object)
                            {
                                // Verificar se tem sensor_type (formato estruturado)
                                if (record.TryGetProperty("data", out var dataElement) && 
                                    record.TryGetProperty("sensor_type", out var sensorElement))
                                {
                                    var sensorType = sensorElement.GetString();
                                    var dataValue = dataElement.GetString();
                                    
                                    Console.WriteLine($"[BD-PARSE] Record com sensor_type: {sensorType} = {dataValue}");
                                    
                                    if (!string.IsNullOrEmpty(sensorType) && 
                                        !string.IsNullOrEmpty(dataValue) && 
                                        double.TryParse(dataValue, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double recordValue))
                                    {
                                        result[sensorType] = recordValue;
                                        Console.WriteLine($"[BD-PARSE] ✓ Valor extraído: {sensorType} = {recordValue}");
                                    }
                                }
                                // Se só tem data, usar o tópico do JSON pai para identificar o tipo
                                else if (record.TryGetProperty("data", out var dataElement2))
                                {
                                    var dataValue = dataElement2.GetString();
                                    Console.WriteLine($"[BD-PARSE] Record só com data: {dataValue}");
                                    
                                    // Usar o campo 'topic' do objeto pai para determinar o tipo de sensor
                                    if (element.TryGetProperty("topic", out var topicElement))
                                    {
                                        var topic = topicElement.GetString();
                                        Console.WriteLine($"[BD-PARSE] Usando topic como tipo de sensor: {topic}");
                                        
                                        if (!string.IsNullOrEmpty(topic) && 
                                            !string.IsNullOrEmpty(dataValue) && 
                                            double.TryParse(dataValue, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double recordValue))
                                        {
                                            result[topic] = recordValue;
                                            Console.WriteLine($"[BD-PARSE] ✓ Valor extraído usando topic: {topic} = {recordValue}");
                                        }
                                    }
                                }
                                else
                                {
                                    // Tentar parsing direto do objeto record
                                    ParseJsonObject(record, result);
                                }
                            }
                        }
                    }
                    else if (prop.Value.ValueKind == JsonValueKind.Array)
                    {
                        // Para outros arrays, tentar processar cada elemento
                        Console.WriteLine($"[BD-PARSE] Processando array genérico '{prop.Name}'...");
                        foreach (var item in prop.Value.EnumerateArray())
                        {
                            if (item.ValueKind == JsonValueKind.Object)
                            {
                                ParseJsonObject(item, result);
                            }
                            else if (item.ValueKind == JsonValueKind.Number)
                            {
                                // Se for um array de números, usar o nome da propriedade
                                result[prop.Name] = item.GetDouble();
                                Console.WriteLine($"[BD-PARSE] Array número: {prop.Name} = {item.GetDouble()}");
                                break; // Pegar apenas o primeiro valor
                            }
                            else if (item.ValueKind == JsonValueKind.String)
                            {
                                var itemStr = item.GetString();
                                if (double.TryParse(itemStr, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double itemVal))
                                {
                                    result[prop.Name] = itemVal;
                                    Console.WriteLine($"[BD-PARSE] Array string: {prop.Name} = {itemVal}");
                                    break;
                                }
                            }
                        }
                    }
                    else if (prop.Value.ValueKind == JsonValueKind.Object)
                    {
                        // Processar objetos aninhados recursivamente
                        Console.WriteLine($"[BD-PARSE] Processando objeto aninhado: {prop.Name}");
                        ParseJsonObject(prop.Value, result);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[BD-PARSE] Erro ao processar propriedade {prop.Name}: {ex.Message}");
                }
            }
        }
        
        public async Task<bool> CreateTestDataAsync()
        {
            try
            {
                Console.WriteLine("[BD-TEST] Criando dados de teste na base de dados...");
                
                var random = new Random();
                var wavyIds = new[] { "WAVY001", "WAVY002", "WAVY003" };
                var now = DateTime.UtcNow;
                
                var testData = new List<WavyData>();
                
                // Criar 20 registros de teste com dados variados
                for (int i = 0; i < 20; i++)
                {
                    foreach (var wavyId in wavyIds)
                    {
                        var timestamp = now.AddMinutes(-i * 5); // Dados a cada 5 minutos
                        
                        var data = new WavyData
                        {
                            WavyId = wavyId,
                            SensorType = "oceanic_test",
                            Timestamp = timestamp,
                            ReceivedAt = timestamp.AddSeconds(random.Next(1, 10)),
                            ServerInstance = Environment.MachineName,
                            PreprocessingApplied = "test_data",
                            RawData = $"{{\"Hs\":{(random.NextDouble() * 3 + 0.5):F2},\"Hmax\":{(random.NextDouble() * 5 + 1):F2},\"SST\":{(random.NextDouble() * 5 + 18):F1},\"Tp\":{(random.NextDouble() * 8 + 4):F1},\"Tz\":{(random.NextDouble() * 6 + 3):F1}}}",
                            
                            // Valores diretos para garantir que apareçam
                            Hs = Math.Round(random.NextDouble() * 3 + 0.5, 2), // 0.5 a 3.5 metros
                            Hmax = Math.Round(random.NextDouble() * 5 + 1, 2), // 1.0 a 6.0 metros
                            SST = Math.Round(random.NextDouble() * 5 + 18, 1), // 18.0 a 23.0 °C
                            Tp = Math.Round(random.NextDouble() * 8 + 4, 1), // 4.0 a 12.0 segundos
                            Tz = Math.Round(random.NextDouble() * 6 + 3, 1), // 3.0 a 9.0 segundos
                            PeakDirection = Math.Round(random.NextDouble() * 360, 0) // 0 a 360 graus
                        };
                        
                        testData.Add(data);
                    }
                }
                
                // Salvar todos os dados
                _context.WavyData.AddRange(testData);
                var saved = await _context.SaveChangesAsync();
                
                Console.WriteLine($"[BD-TEST] ✅ Criados {saved} registros de teste na base de dados");
                
                // Também criar algumas análises de teste
                await CreateTestAnalysisResultsAsync(wavyIds);
                
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[BD-TEST] ❌ Erro ao criar dados de teste: {ex.Message}");
                return false;
            }
        }
        
        private async Task CreateTestAnalysisResultsAsync(string[] wavyIds)
        {
            try
            {
                var random = new Random();
                var patterns = new[] { "stable", "increasing", "decreasing", "volatile", "variable" };
                var analysisData = new List<AnalysisResult>();
                
                // Criar 10 análises de teste
                for (int i = 0; i < 10; i++)
                {
                    foreach (var wavyId in wavyIds)
                    {
                        var analysis = new AnalysisResult
                        {
                            WavyId = wavyId,
                            AnalysisTimestamp = DateTime.UtcNow.AddMinutes(-i * 10),
                            Mean = Math.Round(random.NextDouble() * 2 + 1, 2),
                            StandardDeviation = Math.Round(random.NextDouble() * 0.5 + 0.1, 3),
                            Pattern = patterns[random.Next(patterns.Length)],
                            AnalyzedData = "test_analysis_data",
                            SampleCount = random.Next(3, 10),
                            AnalysisType = "statistical"
                        };
                        
                        analysisData.Add(analysis);
                    }
                }
                
                _context.AnalysisResults.AddRange(analysisData);
                var saved = await _context.SaveChangesAsync();
                
                Console.WriteLine($"[BD-TEST] ✅ Criadas {saved} análises de teste na base de dados");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[BD-TEST] ❌ Erro ao criar análises de teste: {ex.Message}");
            }
        }
        
        public async Task ReprocessExistingDataAsync()
        {
            try
            {
                Console.WriteLine("[BD-REPROCESS] Reprocessando dados existentes para extrair valores...");
                
                // Buscar todos os registros que têm RawData mas valores nulos
                var recordsToUpdate = await _context.WavyData
                    .Where(w => !string.IsNullOrEmpty(w.RawData) && 
                               (!w.Hs.HasValue && !w.Hmax.HasValue && !w.SST.HasValue))
                    .ToListAsync();
                
                Console.WriteLine($"[BD-REPROCESS] Encontrados {recordsToUpdate.Count} registros para reprocessar");
                
                int updated = 0;
                foreach (var record in recordsToUpdate)
                {
                    try
                    {
                        Console.WriteLine($"[BD-REPROCESS] Reprocessando {record.WavyId} - {record.Id}");
                        Console.WriteLine($"[BD-REPROCESS] RawData: {record.RawData}");
                        
                        var parsedData = ParseWavyData(record.RawData);
                        
                        bool hasUpdates = false;
                        
                        if (parsedData.ContainsKey("Hs") && !record.Hs.HasValue)
                        {
                            record.Hs = parsedData["Hs"];
                            hasUpdates = true;
                            Console.WriteLine($"[BD-REPROCESS] ✓ Atualizando Hs: {parsedData["Hs"]}");
                        }
                        
                        if (parsedData.ContainsKey("Hmax") && !record.Hmax.HasValue)
                        {
                            record.Hmax = parsedData["Hmax"];
                            hasUpdates = true;
                            Console.WriteLine($"[BD-REPROCESS] ✓ Atualizando Hmax: {parsedData["Hmax"]}");
                        }
                        
                        if (parsedData.ContainsKey("SST") && !record.SST.HasValue)
                        {
                            record.SST = parsedData["SST"];
                            hasUpdates = true;
                            Console.WriteLine($"[BD-REPROCESS] ✓ Atualizando SST: {parsedData["SST"]}");
                        }
                        
                        if (parsedData.ContainsKey("Tp") && !record.Tp.HasValue)
                        {
                            record.Tp = parsedData["Tp"];
                            hasUpdates = true;
                            Console.WriteLine($"[BD-REPROCESS] ✓ Atualizando Tp: {parsedData["Tp"]}");
                        }
                        
                        if (parsedData.ContainsKey("Tz") && !record.Tz.HasValue)
                        {
                            record.Tz = parsedData["Tz"];
                            hasUpdates = true;
                            Console.WriteLine($"[BD-REPROCESS] ✓ Atualizando Tz: {parsedData["Tz"]}");
                        }
                        
                        if (hasUpdates)
                        {
                            _context.WavyData.Update(record);
                            updated++;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[BD-REPROCESS] ❌ Erro ao reprocessar registro {record.Id}: {ex.Message}");
                    }
                }
                
                if (updated > 0)
                {
                    await _context.SaveChangesAsync();
                    Console.WriteLine($"[BD-REPROCESS] ✅ {updated} registros atualizados com sucesso");
                }
                else                {
                    Console.WriteLine("[BD-REPROCESS] Nenhum registro precisou ser atualizado");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[BD-REPROCESS] ❌ Erro geral no reprocessamento: {ex.Message}");
            }
        }        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _context?.Dispose();
                }
                _disposed = true;
            }
        }        public async Task<int> GetTotalRecordsAsync()
        {
            try
            {
                return await _context.WavyData.CountAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[BD] Erro ao contar registros: {ex.Message}");
                return 0;
            }
        }

        public async Task<int> GetRecordsWithValuesAsync()
        {
            try
            {
                return await _context.WavyData
                    .Where(w => w.Hs != null || w.Hmax != null || w.SST != null || w.Tp != null || w.Tz != null)
                    .CountAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[BD] Erro ao contar registros com valores: {ex.Message}");
                return 0;
            }
        }        public async Task<List<dynamic>> GetRecentDataWithValuesAsync(int limit)
        {
            var results = new List<dynamic>();
            
            try
            {
                var data = await _context.WavyData
                    .Where(w => w.Hs != null || w.Hmax != null || w.SST != null)
                    .OrderByDescending(w => w.Timestamp)
                    .Take(limit)
                    .ToListAsync();
                
                foreach (var item in data)
                {
                    results.Add(new
                    {
                        Timestamp = item.Timestamp,
                        WavyId = item.WavyId ?? "",
                        Hs = item.Hs,
                        Hmax = item.Hmax,
                        SST = item.SST,
                        Tp = item.Tp,
                        Tz = item.Tz
                    });
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[BD] Erro ao buscar dados recentes: {ex.Message}");
            }
            
            return results;
        }

        public async Task<List<string>> GetSampleRawDataAsync(int limit)
        {
            var results = new List<string>();
            
            try
            {
                var data = await _context.WavyData
                    .Where(w => !string.IsNullOrEmpty(w.RawData))
                    .OrderByDescending(w => w.Timestamp)
                    .Take(limit)
                    .Select(w => w.RawData!)
                    .ToListAsync();
                
                results.AddRange(data);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[BD] Erro ao buscar rawData: {ex.Message}");
            }
            
            return results;
        }

        public async Task<List<string>> GetRawDataSamplesAsync(int limit)
        {
            return await GetSampleRawDataAsync(limit);
        }        public Dictionary<string, double> TestParseDataFromRaw(string rawData)
        {
            // Usar o método existente mas com logs mais detalhados
            Console.WriteLine($"[PARSE-TEST] Testando parsing de: {rawData.Substring(0, Math.Min(100, rawData.Length))}...");
            var result = ParseWavyData(rawData);
            Console.WriteLine($"[PARSE-TEST] Resultado: {result.Count} valores extraídos");
            return result;
        }
    }
}
