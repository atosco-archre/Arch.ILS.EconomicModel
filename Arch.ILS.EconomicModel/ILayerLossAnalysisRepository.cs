
namespace Arch.ILS.EconomicModel
{
    public interface ILayerLossAnalysisRepository
    {
        Task<IEnumerable<LayerLossAnalysis>> GetLayerLossAnalyses();

        Task<IEnumerable<LayerLossAnalysis>> GetLayerLossAnalyses(long afterRowVersion);
    }
}
