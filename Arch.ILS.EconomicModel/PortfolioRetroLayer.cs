
using Studio.Core;

namespace Arch.ILS.EconomicModel
{
    public class PortfolioRetroLayer : IRecord
    {
        [Field(0)]
        public int PortLayerId { get; set; }
        [Field(1)]
        public int LayerId { get; set; }
        [Field(2)]
        public int PortfolioId { get; set; }
        [Field(3)]
        public int RetroProgramId { get; set; }
        [Field(4)]
        public long RowVersion { get; set; }
    }
}
