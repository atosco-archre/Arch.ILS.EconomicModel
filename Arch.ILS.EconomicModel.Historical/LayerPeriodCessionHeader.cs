using LargeLoss.LayeringService.Client;

namespace Arch.ILS.EconomicModel.Historical
{
    public class LayerPeriodCessionHeader
    {
        public LayerPeriodCessionHeader(string layerPeriodCessionHeaderName, DateTime asOfDate) 
        {
            LayerPeriodCessionHeaderName = layerPeriodCessionHeaderName;
            AsOfDate = asOfDate.ToUniversalTime();
        }

        public int LayerPeriodCessionHeaderId { get; set; }
        public string LayerPeriodCessionHeaderName { get; }
        public DateTime AsOfDate { get; }
    }
}
