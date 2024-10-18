
namespace Arch.ILS.EconomicModel.Historical
{
    public class ScenarioLossEvent
    {
        public long ScenarioId { get; set; }
        public string EventKey { get; set; }
        public string Name { get; set; }
        public string AutoName { get; set; }
        public DateTime EventDate { get; set; }
        public string LossCurrency { get; set; }
        public decimal? TrendedLoss { get; set; }
        public decimal? UntrendedLoss { get; set; }
        public int EventYear { get; set; }
        public string PerilCode { get; set; }
        public string PerilName { get; set; }
        public bool IsActive { get; set; }
        public bool IsDeleted { get; set; }
        public DateTime CreatedDate { get; set; }
        public string CreatedBy { get; set; }
        public DateTime? ModifiedDate { get; set; }
        public string ModifiedBy { get; set; }
        public string DtoLossEventJson { get; set; }
    }
}
