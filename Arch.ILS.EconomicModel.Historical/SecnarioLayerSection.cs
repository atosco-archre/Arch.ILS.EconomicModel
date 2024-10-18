
namespace Arch.ILS.EconomicModel.Historical
{
    public class ScenarioLayerSection
    {
        public long ScenarioId { get; set; }
        public long LayerId { get; set; }
        public long SectionId { get; set; }
        public int RollUpType { get; set; }
        public decimal FXRateToParent { get; set; }
    }
}