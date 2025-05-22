using System.Threading.Tasks;
using Grpc.Core;
using AnalysisService.Protos;

namespace AnalysisService.Services
{
    public class DataAnalysisService : DataAnalysis.DataAnalysisBase
    {
        public override Task<AnalysisResult> Analyze(DataRequest request, ServerCallContext context)
        {
            var values = request.Values;
            double mean = 0, stddev = 0;
            string pattern = "random";

            if (values.Count > 0)
            {
                mean = values.Average();
                stddev = Math.Sqrt(values.Select(v => Math.Pow(v - mean, 2)).Average());
                if (values.SequenceEqual(values.OrderBy(x => x)))
                    pattern = "increasing";
                else if (values.SequenceEqual(values.OrderByDescending(x => x)))
                    pattern = "decreasing";
            }

            return Task.FromResult(new AnalysisResult
            {
                Mean = mean,
                Stddev = stddev,
                Pattern = pattern
            });
        }
    }
}