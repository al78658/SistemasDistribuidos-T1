using Grpc.Core;
using PreProcessingService;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Xml;

namespace PreProcessingService.Services
{
    public class GreeterService : Greeter.GreeterBase
    {
        private readonly ILogger<GreeterService> _logger;
        
        public GreeterService(ILogger<GreeterService> logger)
        {
            _logger = logger;
        }

        public override Task<HelloReply> SayHello(HelloRequest request, ServerCallContext context)
        {
            return Task.FromResult(new HelloReply
            {
                Message = "Hello " + request.Name
            });
        }
        
        public override Task<ProcessDataReply> ProcessData(ProcessDataRequest request, ServerCallContext context)
        {
            _logger.LogInformation($"Processing data for WAVY {request.WavyId} from {request.SourceFormat} to {request.TargetFormat}");
            
            try
            {
                // Parse the input data based on source format
                var parsedData = ParseData(request.Data, request.SourceFormat);
                
                // Convert to target format
                string processedData = ConvertToFormat(parsedData, request.TargetFormat);
                
                return Task.FromResult(new ProcessDataReply
                {
                    ProcessedData = processedData,
                    Success = true,
                    PreprocessingApplied = $"Conversion from {request.SourceFormat} to {request.TargetFormat}"
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error processing data for WAVY {request.WavyId}");
                return Task.FromResult(new ProcessDataReply
                {
                    Success = false,
                    ErrorMessage = ex.Message,
                    PreprocessingApplied = "Error during processing"
                });
            }
        }
        
        public override Task<StandardizeRateReply> StandardizeReadingRate(StandardizeRateRequest request, ServerCallContext context)
        {
            _logger.LogInformation($"Standardizing reading rate for WAVY {request.WavyId} from {request.SourceInterval} to {request.TargetInterval}");
            
            try
            {
                // Parse the data (assuming CSV format for simplicity)
                var readings = ParseReadings(request.Data);
                
                // Standardize the reading rate
                var standardizedReadings = StandardizeReadings(
                    readings, 
                    request.SourceInterval, 
                    request.TargetInterval,
                    request.CustomIntervalSeconds);
                
                // Convert back to string format
                string result = ConvertReadingsToString(standardizedReadings);
                
                return Task.FromResult(new StandardizeRateReply
                {
                    StandardizedData = result,
                    Success = true
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error standardizing reading rate for WAVY {request.WavyId}");
                return Task.FromResult(new StandardizeRateReply
                {
                    Success = false,
                    ErrorMessage = ex.Message
                });
            }
        }
        
        #region Data Format Conversion
        
        private List<Dictionary<string, object>> ParseData(string data, DataFormat format)
        {
            return format switch
            {
                DataFormat.Text => ParseTextData(data),
                DataFormat.Csv => ParseCsvData(data),
                DataFormat.Xml => ParseXmlData(data),
                DataFormat.Json => ParseJsonData(data),
                _ => throw new ArgumentException($"Unsupported format: {format}")
            };
        }
        
        private List<Dictionary<string, object>> ParseTextData(string data)
        {
            var result = new List<Dictionary<string, object>>();
            _logger.LogInformation($"Parsing text data as JSON container: {data.Substring(0, Math.Min(200, data.Length))}...");

            try
            {
                using (JsonDocument doc = JsonDocument.Parse(data))
                {
                    JsonElement root = doc.RootElement;
                    string wavyId = root.GetProperty("wavy_id").GetString();
                    string topic = root.GetProperty("topic").GetString();
                    JsonElement records = root.GetProperty("records");

                    foreach (JsonElement record in records.EnumerateArray())
                    {
                        var entry = new Dictionary<string, object>
                        {
                            { "wavy_id", wavyId },
                            { "topic", topic },
                            { "data", record.GetString() },
                            { "timestamp", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") } // Atribui um novo timestamp no processamento
                        };
                        result.Add(entry);
                    }
                }
            }
            catch (JsonException ex)
            {
                _logger.LogError(ex, "Falha ao analisar os dados de texto recebidos como o formato de contêiner JSON esperado.");
                // Se falhar, não podemos processá-lo, então lançamos uma exceção.
                // Lançar é melhor, pois será capturado pelo método chamador e relatado como uma falha.
                throw new ArgumentException("Os dados de entrada não estão no formato de contêiner JSON esperado.", ex);
            }

            _logger.LogInformation($"Parsed {result.Count} records from text data");

            return result;
        }
        
        private List<Dictionary<string, object>> ParseCsvData(string data)
        {
            var result = new List<Dictionary<string, object>>();
            var lines = data.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            
            if (lines.Length < 2)
                return result;
                
            var headers = lines[0].Split(',');
            
            for (int i = 1; i < lines.Length; i++)
            {
                var values = lines[i].Split(',');
                if (values.Length != headers.Length)
                    continue;
                    
                var entry = new Dictionary<string, object>();
                
                for (int j = 0; j < headers.Length; j++)
                {
                    entry[headers[j]] = values[j];
                }
                
                result.Add(entry);
            }
            
            return result;
        }
        
        private List<Dictionary<string, object>> ParseXmlData(string data)
        {
            var result = new List<Dictionary<string, object>>();
            
            try
            {
                var doc = new XmlDocument();
                doc.LoadXml(data);
                
                var readings = doc.SelectNodes("//reading");
                if (readings != null)
                {
                    foreach (XmlNode reading in readings)
                    {
                        var entry = new Dictionary<string, object>();
                        
                        foreach (XmlNode child in reading.ChildNodes)
                        {
                            if (child.NodeType == XmlNodeType.Element)
                            {
                                entry[child.Name] = child.InnerText;
                            }
                        }
                        
                        result.Add(entry);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Error parsing XML data: {ex.Message}");
            }
            
            return result;
        }
        
        private List<Dictionary<string, object>> ParseJsonData(string data)
        {
            try
            {
                // Clean up the data if it contains escaped quotes
                if (data.Contains("\\u0022"))
                {
                    data = data.Replace("\\u0022", "\"");
                }
                
                // Try to parse as a JSON array
                try
                {
                    var options = new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    };
                    
                    var jsonDoc = JsonDocument.Parse(data);
                    if (jsonDoc.RootElement.ValueKind == JsonValueKind.Array)
                    {
                        var result = new List<Dictionary<string, object>>();
                        foreach (var item in jsonDoc.RootElement.EnumerateArray())
                        {
                            var entry = new Dictionary<string, object>();
                            foreach (var prop in item.EnumerateObject())
                            {
                                entry[prop.Name] = prop.Value.ToString();
                            }
                            result.Add(entry);
                        }
                        return result;
                    }
                }
                catch
                {
                    // Continue to other parsing methods
                }
                
                // Try parsing as a single object
                try
                {
                    var jsonDoc = JsonDocument.Parse(data);
                    var root = jsonDoc.RootElement;
                    
                    if (root.ValueKind == JsonValueKind.Object)
                    {
                        // Create a single entry
                        var entry = new Dictionary<string, object>();
                        foreach (var prop in root.EnumerateObject())
                        {
                            entry[prop.Name] = prop.Value.ToString();
                        }
                        return new List<Dictionary<string, object>> { entry };
                    }
                }
                catch
                {
                    // Continue to other parsing methods
                }
                
                // Try parsing as a string that contains multiple JSON objects
                try
                {
                    var lines = data.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                    var result = new List<Dictionary<string, object>>();
                    
                    foreach (var line in lines)
                    {
                        try
                        {
                            var jsonDoc = JsonDocument.Parse(line.Trim());
                            var entry = new Dictionary<string, object>();
                            
                            if (jsonDoc.RootElement.ValueKind == JsonValueKind.Object)
                            {
                                foreach (var prop in jsonDoc.RootElement.EnumerateObject())
                                {
                                    entry[prop.Name] = prop.Value.ToString();
                                }
                                result.Add(entry);
                            }
                        }
                        catch
                        {
                            // Skip invalid lines
                        }
                    }
                    
                    if (result.Count > 0)
                    {
                        return result;
                    }
                }
                catch
                {
                    // Continue to final exception
                }
                
                throw new Exception("Invalid JSON format");
            }
            catch (Exception ex)
            {
                throw new Exception($"Error parsing JSON data: {ex.Message}");
            }
        }
        
        private string ConvertToFormat(List<Dictionary<string, object>> data, DataFormat format)
        {
            return format switch
            {
                DataFormat.Text => ConvertToText(data),
                DataFormat.Csv => ConvertToCsv(data),
                DataFormat.Xml => ConvertToXml(data),
                DataFormat.Json => ConvertToJson(data),
                _ => throw new ArgumentException($"Unsupported format: {format}")
            };
        }
        
        private string ConvertToText(List<Dictionary<string, object>> data)
        {
            var sb = new StringBuilder();
            
            foreach (var entry in data)
            {
                if (entry.TryGetValue("timestamp", out var timestamp))
                {
                    sb.Append(timestamp);
                    
                    foreach (var kvp in entry.Where(x => x.Key != "timestamp"))
                    {
                        sb.Append(' ').Append(kvp.Value);
                    }
                    
                    sb.Append(" | ");
                }
            }
            
            // Remove the last separator
            if (sb.Length >= 3)
                sb.Length -= 3;
                
            return sb.ToString();
        }
        
        private string ConvertToCsv(List<Dictionary<string, object>> data)
        {
            if (data.Count == 0)
                return string.Empty;
                
            var sb = new StringBuilder();
            
            // Headers
            sb.AppendLine(string.Join(",", data[0].Keys));
            
            // Data rows
            foreach (var entry in data)
            {
                sb.AppendLine(string.Join(",", entry.Values));
            }
            
            return sb.ToString();
        }
        
        private string ConvertToXml(List<Dictionary<string, object>> data)
        {
            var sb = new StringBuilder();
            sb.AppendLine("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            sb.AppendLine("<readings>");
            
            foreach (var entry in data)
            {
                sb.AppendLine("  <reading>");
                
                foreach (var kvp in entry)
                {
                    sb.AppendLine($"    <{kvp.Key}>{kvp.Value}</{kvp.Key}>");
                }
                
                sb.AppendLine("  </reading>");
            }
            
            sb.AppendLine("</readings>");
            return sb.ToString();
        }
        
        private string ConvertToJson(List<Dictionary<string, object>> data)
        {
            if (data == null || !data.Any())
            {
                _logger.LogWarning("ConvertToFormat received empty or null data for JSON conversion.");
                return "{}";
            }

            var firstRecord = data.First();
            string wavyId = firstRecord.TryGetValue("wavy_id", out var id) ? id?.ToString() : "unknown";
            string topic = firstRecord.TryGetValue("topic", out var t) ? t?.ToString() : "unknown";

            // Create a list of objects for the "records" array.
            var recordsList = data.Select(entry => new
            {
                data = entry.TryGetValue("data", out var d) ? d?.ToString() : null,
                timestamp = entry.TryGetValue("timestamp", out var ts) ? ts?.ToString() : null
            }).ToList();

            // Construct the final JSON object that the TCP server expects.
            var finalObject = new
            {
                wavy_id = wavyId,
                topic = topic,
                timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), // A new timestamp for the entire batch.
                records = recordsList // This will be serialized as a proper JSON array of objects.
            };

            var options = new JsonSerializerOptions
            {
                WriteIndented = false, // Compact format for network transmission.
                PropertyNamingPolicy = null, // Keep property names like "wavy_id".
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
            };

            return JsonSerializer.Serialize(finalObject, options);
        }
        
        #endregion
        
        #region Reading Rate Standardization
        
        private class Reading
        {
            public DateTime Timestamp { get; set; }
            public Dictionary<string, double> Values { get; set; }
        }
        
        private List<Reading> ParseReadings(string data)
        {
            var result = new List<Reading>();
            var lines = data.Split(new[] { '|' }, StringSplitOptions.RemoveEmptyEntries);
            
            foreach (var line in lines)
            {
                var parts = line.Trim().Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length < 2)
                    continue;
                    
                if (!DateTime.TryParse(parts[0], out var timestamp))
                    continue;
                    
                var values = new Dictionary<string, double>();
                
                for (int i = 1; i < parts.Length; i++)
                {
                    if (double.TryParse(parts[i], NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                    {
                        values[$"value{i}"] = value;
                    }
                }
                
                result.Add(new Reading
                {
                    Timestamp = timestamp,
                    Values = values
                });
            }
            
            return result;
        }
        
        private List<Reading> StandardizeReadings(
            List<Reading> readings, 
            ReadingInterval sourceInterval, 
            ReadingInterval targetInterval,
            int customIntervalSeconds)
        {
            if (readings.Count == 0)
                return readings;
                
            // Sort readings by timestamp
            readings = readings.OrderBy(r => r.Timestamp).ToList();
            
            // Calculate source interval in seconds
            int sourceIntervalSeconds = GetIntervalInSeconds(sourceInterval, customIntervalSeconds);
            
            // Calculate target interval in seconds
            int targetIntervalSeconds = GetIntervalInSeconds(targetInterval, customIntervalSeconds);
            
            // If source and target intervals are the same, return the original readings
            if (sourceIntervalSeconds == targetIntervalSeconds)
                return readings;
                
            var result = new List<Reading>();
            
            // Determine the start and end times
            DateTime startTime = readings.First().Timestamp;
            DateTime endTime = readings.Last().Timestamp;
            
            // Generate timestamps for the target interval
            for (DateTime time = startTime; time <= endTime; time = time.AddSeconds(targetIntervalSeconds))
            {
                // Find readings that fall within this interval
                var relevantReadings = readings
                    .Where(r => r.Timestamp >= time && r.Timestamp < time.AddSeconds(targetIntervalSeconds))
                    .ToList();
                
                if (relevantReadings.Count == 0)
                    continue;
                    
                // Aggregate the values (using average)
                var aggregatedValues = new Dictionary<string, double>();
                
                foreach (var key in relevantReadings.First().Values.Keys)
                {
                    aggregatedValues[key] = relevantReadings.Average(r => r.Values.ContainsKey(key) ? r.Values[key] : 0);
                }
                
                result.Add(new Reading
                {
                    Timestamp = time,
                    Values = aggregatedValues
                });
            }
            
            return result;
        }
        
        private int GetIntervalInSeconds(ReadingInterval interval, int customIntervalSeconds)
        {
            return interval switch
            {
                ReadingInterval.PerSecond => 1,
                ReadingInterval.PerMinute => 60,
                ReadingInterval.PerHour => 3600,
                ReadingInterval.Custom => customIntervalSeconds > 0 ? customIntervalSeconds : 60,
                _ => 60 // Default to per minute
            };
        }
        
        private string ConvertReadingsToString(List<Reading> readings)
        {
            var sb = new StringBuilder();
            
            foreach (var reading in readings)
            {
                sb.Append(reading.Timestamp.ToString("yyyy-MM-dd HH:mm:ss"));
                
                foreach (var value in reading.Values.Values)
                {
                    sb.Append(' ').Append(value.ToString(CultureInfo.InvariantCulture));
                }
                
                sb.Append(" | ");
            }
            
            // Remove the last separator
            if (sb.Length >= 3)
                sb.Length -= 3;
                
            return sb.ToString();
        }
        
        #endregion
    }
}
