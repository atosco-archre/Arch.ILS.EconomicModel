
namespace Arch.ILS.EconomicModel.Stochastic
{
    public interface IMixedRepository
    {
        IEnumerable<LayerActualQuarterMetrics> GetRetroLayerActualITDQuarterMetrics(int retroProgramId, int acctGPeriod);
        IEnumerable<LayerActualMetrics> GetRetroLayerActualITDMetrics(int retroProgramId, int acctGPeriod);
        void AddCalculationHeader(in int calculationId, in string calculationName, in int acctGPeriod, in DateTime asAtDate, in bool useBoundFx, in string baseCurrency, in DateTime currentFXDate);
        void BulkLoadLayerItdMetrics(in string filePath, in string fileNameWithExtension);

        void BulkLoadOriginalYelt(in string filePath, in string fileNameWithExtension);

        void BulkLoadRecalculatedYelt(in string filePath, in string fileNameWithExtension);

        void BulkLoadConditionalYelt(in string filePath, in string fileNameWithExtension);

        void BulkLoadRetroCessionMetrics(in string filePath, in string fileNameWithExtension);

        void BulkLoadRetroLayerCessionMetrics(in string filePath, in string fileNameWithExtension);
    }
}
