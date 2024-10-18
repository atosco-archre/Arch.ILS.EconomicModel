
namespace Arch.ILS.EconomicModel.Historical
{
    public class ScenarioRetroCessionLoss
    {
        public long ScenarioId { get; set; }
        public string EventKey { get; set; }
        public long RetroAllocationId { get; set; }
        public long LayerId { get; set; }
        public long RetroInvestorId { get; set; }
        public string Currency { get; set; }
        public decimal? Loss { get; set; }
        public decimal? OccLoss { get; set; }
        public decimal? ReinstPremium { get; set; }
    }
}
