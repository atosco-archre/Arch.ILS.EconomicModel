
using System.ComponentModel;

namespace Arch.ILS.EconomicModel
{
    public enum RegisPerspectiveType : byte
    {
        [Description("Assumed")]
        Assumed = 1,

        [Description("Ceded")]
        Ceded = 2,

        [Description("Bermuda Cession")]
        Bermuda_Cession = 3,

        [Description("LPT Cession")]
        LPT_Cession = 4
    }
}
