
using Studio.Core;

namespace Arch.EconomicModel
{
    public class Portfolio : IRecord
    {
        [Field(0)]
        public int PortfolioId { get; set; }
        [Field(1)]
        public int PortfolioType { get; set; }
        [Field(2)]
        public DateTime AsOfDate { get; set; }
    }
}
