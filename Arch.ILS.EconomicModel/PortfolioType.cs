
using System.ComponentModel;

namespace Arch.ILS.EconomicModel
{
    public enum PortfolioType : byte
    {
        [Description("In Force")]
        InForce = 0,

        [Description("Next 12 Months")]
        Next12Months = 1,

        [Description("Current Year")]
        CurrentYear = 2,

        [Description("Next Year")]
        NextYear = 3
    }
}
