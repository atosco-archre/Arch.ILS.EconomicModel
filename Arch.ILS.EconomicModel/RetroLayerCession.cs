
namespace Arch.ILS.EconomicModel
{
    public sealed class RetroLayerCession
    {
        public int RetroProgramId { get; set; }
        public int LayerId { get; set; }
        public int RetroProgramResetId { get; set; }
        public decimal CessionGross { get; set; }
        public byte RetroLevelType { get; set; }
        public DateTime OverlapStart { get; set; }
        public DateTime OverlapEnd { get; set; }
        public ResetType ResetType { get; set; }
    }
}
