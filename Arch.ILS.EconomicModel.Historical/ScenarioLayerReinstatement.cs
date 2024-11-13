
namespace Arch.ILS.EconomicModel.Historical
{
    public record class ScenarioLayerReinstatement(long ScenarioId, long LayerId, int ReinstatementId, int ReinstatementOrder, double Quantity, decimal PremiumShare, decimal BrokeragePercentage, byte[] RowVersion);
}
