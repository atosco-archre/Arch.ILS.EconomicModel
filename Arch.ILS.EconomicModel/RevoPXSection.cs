
using Studio.Core;

namespace Arch.ILS.EconomicModel
{
    public class RevoPXSection : IRecord
    {
        [Field(0)]
        public int LayerId { get; set; }
        [Field(1)]
        public int PXLayerId { get; set; }
        [Field(2)]
        public RevoRollUpType Rollup { get; set; }
        [Field(3)]
        public long RowVersion { get; set; }
        public double FXToParent => 1;// All the layers in a submission in Revo have the same currency
    }
}
