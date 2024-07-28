
using Studio.Core;

namespace Arch.EconomicModel
{
    public class PortLayer : IRecord
    {
        [Field(0)]
        public int PortLayerId { get;set; }
        [Field(1)]
        public int LayerId { get;set; }
        [Field(2)]
        public int PortfolioId { get;set; }
    }
}
