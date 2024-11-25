
using Studio.Core;

namespace Arch.ILS.EconomicModel
{
    public class RetroLayer : IRecord
    {
        [Field(0)]
        public int LayerId { get; set; }
        [Field(1)]
        public int RetroProgramId { get; set; }
        [Field(2)]
        public long RowVersion { get; set; }
    }
}
