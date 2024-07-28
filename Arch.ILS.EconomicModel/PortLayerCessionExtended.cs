
namespace Arch.ILS.EconomicModel
{
    public sealed class PortLayerCessionExtended : PortLayerCession
    {
        public int PortfolioId { get; set; }
        public int LayerId { get; set; }
        public byte RetroLevelType {  get; set; }
        public DateTime OverlapStart { get; set; }
        public DateTime OverlapEnd { get; set; }
    }
}
