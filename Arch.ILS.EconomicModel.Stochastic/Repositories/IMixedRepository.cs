
namespace Arch.ILS.EconomicModel.Stochastic
{
    public interface IMixedRepository
    {
        IEnumerable<LayerActualQuarterMetrics> GetRetroLayerActualITDQuarterMetrics(int retroProgramId, int acctGPeriod);
        IEnumerable<LayerActualMetrics> GetRetroLayerActualITDMetrics(int retroProgramId, int acctGPeriod);
        int AddCalculationHeader(ConditionalCalculationInputBase input);
        void BulkLoadLayerItdMetrics(in int calculationId, in string filePath, in string fileNameWithExtension);

        void BulkLoadOriginalYelt(in int calculationId, in int layerId, in RevoLossViewType lossView, in string filePath, in string fileNameWithExtension);

        void BulkLoadRecalculatedYelt(in int calculationId, in int layerId, in RevoLossViewType lossView, in string filePath, in string fileNameWithExtension);

        void BulkLoadConditionalYelt(in int calculationId, in int layerId, in RevoLossViewType lossView, in string filePath, in string fileNameWithExtension);

        void BulkLoadRetroCessionMetrics(in int calculationId, in string filePath, in string fileNameWithExtension);

        void BulkLoadRetroLayerCessionMetrics(in int calculationId, in string filePath, in string fileNameWithExtension);
    }
}
