using Grpc.Core;
using PreProcessingService.Protos;
using System.Threading.Tasks;
using System.Text.Json;
using System.Collections.Generic;
using System.Linq;
using System;

namespace PreProcessingService.Services
{
    public class PreProcessingServiceImpl : PreProcessing.PreProcessingBase
    {
        public override Task<PreProcessResponse> PreProcess(PreProcessRequest request, ServerCallContext context)
        {
            // Lógica de pré-processamento realista:
            // 1. Detectar formato (CSV, JSON, XML, texto simples)
            // 2. Converter sempre para JSON estruturado

            string raw = request.RawData?.Trim() ?? "";
            var targetRate = request.TargetRate; // enum TimeRate enviado pelo Agregador
            string resultJson = "";

            try
            {
                if ((raw.StartsWith("[") && raw.EndsWith("]")))
                {
                    // Padronização de taxa de leitura conforme targetRate
                    var jsonArray = JsonSerializer.Deserialize<List<Dictionary<string, object>>>(raw);
                    if (jsonArray != null && jsonArray.Count > 0 && jsonArray[0].ContainsKey("timestamp"))
                    {
                        if (targetRate == ReadingInterval.PerSecond)
                        {
                            resultJson = raw; // manter como está
                        }
                        else
                        {
                            int keyLen = targetRate switch
                            {
                                ReadingInterval.PerSecond => 19, // yyyy-MM-dd HH:mm:ss
                                ReadingInterval.PerMinute => 16, // yyyy-MM-dd HH:mm
                                ReadingInterval.PerHour   => 13, // yyyy-MM-dd HH
                                _               => 19
                            };

                            var agrupado = jsonArray
                                .GroupBy(x => x["timestamp"].ToString().Substring(0, keyLen))
                                .Select(g =>
                                {
                                    var primeiro = g.First();
                                    var result = new Dictionary<string, object>(primeiro);
                                    foreach (var key in primeiro.Keys)
                                    {
                                        if (key != "timestamp" && double.TryParse(primeiro[key]?.ToString(), out _))
                                        {
                                            var media = g.Select(x => double.TryParse(x[key]?.ToString(), out double v) ? v : 0).Average();
                                            result[key] = media;
                                        }
                                    }
                                    result["timestamp"] = g.Key;
                                    return result;
                                })
                                .ToList();
                            resultJson = JsonSerializer.Serialize(agrupado);
                        }
                    }
                    else
                    {
                        // Se não for array de registos válidos, devolver como está
                        resultJson = raw;
                    }
                }
                else if ((raw.StartsWith("{") && raw.EndsWith("}")))
                {
                    // Já está em JSON, validar
                    JsonDocument.Parse(raw);
                    resultJson = raw;
                }
                else if (raw.Contains(",") && raw.Split(',').Length > 1)
                {
                    // CSV simples (ex: "timestamp,1.12,1.74,...")
                    var colunas = new[] { "timestamp", "Hs", "Hmax", "Tz", "Tp", "Peak Direction", "SST" };
                    var valores = raw.Split(',');
                    var obj = new Dictionary<string, string>();
                    for (int i = 0; i < colunas.Length && i < valores.Length; i++)
                        obj[colunas[i]] = valores[i];
                    resultJson = JsonSerializer.Serialize(obj);
                }
                else if (raw.StartsWith("<") && raw.EndsWith(">"))
                {
                    // XML - para simplificação, devolver JSON com campo 'xml'
                    resultJson = JsonSerializer.Serialize(new { xml = raw });
                }
                else
                {
                    // Texto simples
                    resultJson = JsonSerializer.Serialize(new { data = raw });
                }
            }
            catch
            {
                // Se falhar, devolver como texto simples
                resultJson = JsonSerializer.Serialize(new { data = raw });
            }

            string preprocessingApplied;

            // Descrição base conforme formato
            if ((raw.StartsWith("[") && raw.EndsWith("]")))
                preprocessingApplied = targetRate == ReadingInterval.PerSecond ?
                                        "JSON mantido" :
                                        $"Padronização/agregação de JSON por {targetRate.ToString().ToLower()}";
            else if ((raw.StartsWith("{") && raw.EndsWith("}")))
                preprocessingApplied = "Validação de JSON";
            else if (raw.Contains(",") && raw.Split(',').Length > 1)
                preprocessingApplied = "Conversão de CSV simples para JSON";
            else if (raw.StartsWith("<") && raw.EndsWith(">"))
                preprocessingApplied = "Conversão de XML para JSON";
            else
                preprocessingApplied = "Conversão de texto simples para JSON";

            return Task.FromResult(new PreProcessResponse { ProcessedData = resultJson, PreprocessingApplied = preprocessingApplied });
        }
    }
}