
namespace Arch.ILS.EconomicModel
{
    public record class RetroSummaryMetrics(IDictionary<int, RetroMetrics> RetroMetricsByRetroProgramId, IList<RetroLayerMetrics> RetroLayerMetrics);
}
