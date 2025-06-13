using System.Threading.Tasks;
using Grpc.Core;
using AnalysisService.Protos;

namespace AnalysisService.Services
{    public class DataAnalysisService : DataAnalysis.DataAnalysisBase
    {
        public override Task<AnalysisResult> Analyze(DataRequest request, ServerCallContext context)
        {
            Console.WriteLine($"[ANALYSIS] Recebida solicitação de análise de {request.Source} com {request.Values.Count} valores");
            
            var values = request.Values;
            double mean = 0, stddev = 0;
            string pattern = "random";

            if (values.Count > 0)
            {
                mean = values.Average();
                var variance = values.Select(v => Math.Pow(v - mean, 2)).Average();
                stddev = Math.Sqrt(variance);
                
                // Análise de padrões mais detalhada
                if (values.Count > 1)
                {
                    var ordered = values.OrderBy(x => x).ToList();
                    var descending = values.OrderByDescending(x => x).ToList();
                    
                    if (values.SequenceEqual(ordered))
                        pattern = "increasing";
                    else if (values.SequenceEqual(descending))
                        pattern = "decreasing";
                    else if (stddev < mean * 0.1) // Baixa variabilidade
                        pattern = "stable";
                    else if (stddev > mean * 0.5) // Alta variabilidade
                        pattern = "volatile";
                    else
                        pattern = "variable";
                }
            }

            Console.WriteLine($"[ANALYSIS] Análise concluída: Média={mean:F2}, DesvPadrão={stddev:F2}, Padrão={pattern}");

            return Task.FromResult(new AnalysisResult
            {
                Mean = mean,
                Stddev = stddev,
                Pattern = pattern
            });
        }
    }
}