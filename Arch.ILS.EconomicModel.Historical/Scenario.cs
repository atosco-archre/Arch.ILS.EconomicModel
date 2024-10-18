
namespace Arch.ILS.EconomicModel.Historical
{
    public class Scenario
    {
        public long ScenarioId { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public string Notes { get; set; }
        public int AnalysisStatus { get; set; }
        public int ScenarioStatus { get; set; }
        public bool UseAdjustedMarketShare { get; set; }
        public bool UseTrendedLosses { get; set; }
        public string Currency { get; set; }
        public DateTime? InforceDate { get; set; }
        public DateTime? FXRatesDate { get; set; }
        public bool IsActive { get; set; }
        public bool IsDeleted { get; set; }
        public DateTime CreatedDate { get; set; }
        public string CreatedBy { get; set; }
        public DateTime? ModifiedDate { get; set; }
        public string ModifiedBy { get; set; }
        public string StepsJson { get; set; }
        public bool IsOfficial { get; set; }
        public long? ScenarioType { get; set; }
        public DateTime? RunDate { get; set; }
    }
}
