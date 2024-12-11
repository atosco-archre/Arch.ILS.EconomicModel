
namespace Arch.ILS.EconomicModel.Stochastic
{
    public interface IMixedRepository
    {
        IEnumerable<LayerActualMetrics> GetRetroLayerActualITDMetrics(int retroProgramId, int acctGPeriod);

        void BulkCopyRecalculatedYelt(string filePath);
    }
}
