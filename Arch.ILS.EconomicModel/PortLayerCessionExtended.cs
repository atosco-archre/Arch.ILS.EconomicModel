
namespace Arch.ILS.EconomicModel
{
    public sealed class PortLayerCessionExtended : PortLayerCession
    {
        public int PortfolioId;
        public int LayerId;
        public byte RetroLevelType;
        public DateTime OverlapStart;
        public DateTime OverlapEnd;
    }
}
