using Grpc.Core;
using PreProcessingService.Protos;
using System.Threading.Tasks;

namespace PreProcessingService.Services
{
    public class PreProcessingServiceImpl : PreProcessing.PreProcessingBase
    {
        public override Task<PreProcessResponse> PreProcess(PreProcessRequest request, ServerCallContext context)
        {
            // Exemplo simples: transformar os dados em mai�sculas (substitua pela l�gica real)
            var processed = request.RawData.ToUpperInvariant();
            return Task.FromResult(new PreProcessResponse { ProcessedData = processed });
        }
    }
}