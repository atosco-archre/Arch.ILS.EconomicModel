
using Studio.Core;

namespace Arch.ILS.EconomicModel
{
    public class LayerLossAnalysis : IRecord
    {
        [Field(0)]
        public int LossAnalysisId { get; set; }
        [Field(1)]
        public int LayerId { get; set; }
        [Field(2)]
        public RevoLossViewType LossView { get; set; }
        [Field(3)]
        public long RowVersion { get; set; }
    }
}
