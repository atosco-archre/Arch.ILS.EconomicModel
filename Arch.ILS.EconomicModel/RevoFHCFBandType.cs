
using System.ComponentModel;

namespace Arch.ILS.EconomicModel
{
    public enum RevoFHCFBandType : short
    {
        [Description("")]
        None = 0,

        [Description("Below")]
        Below = 1,

        [Description("Along")]
        Along = 2,

        [Description("Above")]
        Above = 3,

        [Description("Other")]
        Other = 4,

        [Description("Below/Along")]
        BelowAlong	= 12,

        [Description("Along/Above")]
        AlongAbove = 23,

        [Description("All")]
        All	= 123
    }
}
