using Grpc.Core;
using PreProcessingService.Protos;
using System.Threading.Tasks;
using System.Text.Json;
using System.Collections.Generic;
using System;
using System.Globalization;
using System.Linq;

namespace PreProcessingService.Services
{
    public class PreProcessingServiceImpl : PreProcessing.PreProcessingBase
    {
        public override Task<PreProcessResponse> PreProcess(PreProcessRequest request, ServerCallContext context)
        {
            try
            {
                Console.WriteLine($"[PREPROCESSING] Processando dados para WAVY: {request.WavyId}");
                Console.WriteLine($"[PREPROCESSING] Dados recebidos: {request.RawData.Substring(0, Math.Min(100, request.RawData.Length))}...");
                
                // Processar dados agregados do formato "topic:value | topic:value | ..."
                var processedData = ProcessarDadosAgregados(request.RawData, request.WavyId);
                
                Console.WriteLine($"[PREPROCESSING] Dados processados: {processedData.Substring(0, Math.Min(100, processedData.Length))}...");
                
                return Task.FromResult(new PreProcessResponse { ProcessedData = processedData });
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[PREPROCESSING] Erro ao processar dados: {ex.Message}");
                // Retornar dados originais em caso de erro
                return Task.FromResult(new PreProcessResponse { ProcessedData = request.RawData });
            }
        }
        
        private string ProcessarDadosAgregados(string dadosAgregados, string wavyId)
        {
            try
            {
                // Dividir os dados por " | " para obter cada entrada
                var entradas = dadosAgregados.Split(" | ", StringSplitOptions.RemoveEmptyEntries);
                var dadosEstruturados = new List<Dictionary<string, object>>();
                
                // Agrupar dados por timestamp (assumindo que cada grupo representa uma leitura completa)
                var gruposPorTimestamp = new Dictionary<string, Dictionary<string, string>>();
                
                foreach (var entrada in entradas)
                {
                    var partes = entrada.Split(':', 2);
                    if (partes.Length == 2)
                    {
                        string topic = partes[0].Trim();
                        string valor = partes[1].Trim();
                        
                        // Usar timestamp atual como chave (pode ser melhorado para extrair timestamp real)
                        string timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                        
                        if (!gruposPorTimestamp.ContainsKey(timestamp))
                        {
                            gruposPorTimestamp[timestamp] = new Dictionary<string, string>();
                        }
                        
                        gruposPorTimestamp[timestamp][topic] = valor;
                    }
                }
                
                // Converter grupos em registros estruturados
                foreach (var grupo in gruposPorTimestamp)
                {
                    var registro = new Dictionary<string, object>
                    {
                        ["timestamp"] = grupo.Key,
                        ["wavy_id"] = wavyId
                    };
                    
                    // Adicionar todos os valores dos tópicos
                    foreach (var topico in grupo.Value)
                    {
                        registro[topico.Key] = topico.Value;
                    }
                    
                    dadosEstruturados.Add(registro);
                }
                
                // Se não conseguimos estruturar os dados, criar um registro simples
                if (dadosEstruturados.Count == 0)
                {
                    var registroSimples = new Dictionary<string, object>
                    {
                        ["timestamp"] = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
                        ["wavy_id"] = wavyId,
                        ["raw_data"] = dadosAgregados
                    };
                    dadosEstruturados.Add(registroSimples);
                }
                
                // Serializar para JSON
                var options = new JsonSerializerOptions
                {
                    WriteIndented = false,
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                };
                
                return JsonSerializer.Serialize(dadosEstruturados, options);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[PREPROCESSING] Erro na estruturação dos dados: {ex.Message}");
                
                // Fallback: retornar dados em formato JSON simples
                var fallbackData = new List<Dictionary<string, object>>
                {
                    new Dictionary<string, object>
                    {
                        ["timestamp"] = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
                        ["wavy_id"] = wavyId,
                        ["processed_data"] = dadosAgregados.ToUpperInvariant()
                    }
                };
                
                return JsonSerializer.Serialize(fallbackData);
            }
        }
    }
}