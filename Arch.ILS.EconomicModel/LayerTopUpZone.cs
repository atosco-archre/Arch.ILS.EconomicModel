using Studio.Core;

namespace Arch.ILS.EconomicModel
{
    public class LayerTopUpZone : IRecord
    {
        [Field(0)]
        public int LayerId { get; set; }
        [Field(1)]
        public int TopUpZoneId { get; set; }
    }
}
