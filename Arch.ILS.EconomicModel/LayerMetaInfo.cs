
using Studio.Core;

namespace Arch.ILS.EconomicModel
{
    public class LayerMetaInfo : IRecord
    {
        [Field(0)]
        public int LayerId { get; set; }

        [Field(1)]
        public SegmentType Segment { get; set; }
    }
}
