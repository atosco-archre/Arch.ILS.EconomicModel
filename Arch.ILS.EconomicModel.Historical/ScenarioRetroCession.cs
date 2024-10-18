
namespace Arch.ILS.EconomicModel.Historical
{
    public class ScenarioRetroCession
    {
        public long ScenarioId { get; set; }
        public long RetroAllocationId { get; set; }
        public long LayerId { get; set; }
        public long RetroInvestorId { get; set; }
        public bool IsActive { get; set; }
        public bool IsDeleted { get; set; }
        public decimal CessionNet { get; set; }
        public decimal CessionGross { get; set; }
        public decimal CessionCapFactor { get; set; }
        public decimal? CessionCapFactorSent { get; set; }
        public decimal? CessionGrossFinalSent { get; set; }
        public decimal? CessionNetFinalSent { get; set; }
        public int AllocationStatus { get; set; }
        public decimal? Override { get; set; }
        public long RetroProgramId { get; set; }
        public string RetroProgramName { get; set; }
        public int RetroLevelType { get; set; }
        public long RetroProfileId { get; set; }
        public string RetroProfileName { get; set; }
        public string RetroInvestorName { get; set; }
        public DateTime? Expiration { get; set; }
        public DateTime? Inception { get; set; }
    }
}
