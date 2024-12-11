
namespace Arch.ILS.EconomicModel
{
    public interface ILayerLossAnalysisRepository
    {
        Task<IEnumerable<LayerLossAnalysis>> GetLayerLossAnalyses();

        Task<IEnumerable<LayerLossAnalysis>> GetLayerLossAnalyses(long afterRowVersion);

        Task<IEnumerable<LayerLossAnalysisExtended>> GetLayerLossAnalysesExtended();

        Task<IEnumerable<LayerLossAnalysisExtended>> GetLayerLossAnalysesExtended(long afterRowVersion);

        Task<IEnumerable<RevoSubmissionGUAnalysis>> GetSubmissionGUAnalyses();
    }
}
