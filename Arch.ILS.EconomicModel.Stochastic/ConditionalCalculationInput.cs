
namespace Arch.ILS.EconomicModel.Stochastic
{
    public record class ConditionalCalculationInput : ConditionalCalculationInputBase
    {
        public bool ApplyErosion { get; set; }
        public HashSet<RevoLossViewType> LossViews { get; set; }
        public HashSet<int> NonGULossBasedLayers { get; set; }
    }
}
