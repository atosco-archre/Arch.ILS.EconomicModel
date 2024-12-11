
using System.ComponentModel;

namespace Arch.ILS.EconomicModel
{
    public enum RevoPeril : byte
    {
        [Description("Building Collapse")]
        BC = 0,

        [Description("Convective Storm")]
        CS = 1,

        [Description("Cyber")]
        CY = 2,
        
        [Description("Earthquake")]
        EQ = 3,
        
        [Description("Flood")]
        FL = 4,
        
        [Description("Fire and Explosion")]
        FE = 5,
        
        [Description("Fire-NonCat")]
        FN = 6,
        
        [Description("Riot")]
        RT = 7,
        
        [Description("Pandemic")]
        PA = 8,
        
        [Description("Power Outage")]
        PO = 9,
        
        [Description("Terrorism")]
        TR = 10,
        
        [Description("Sinkhole")]
        SK = 11,
        
        [Description("Volcanic Eruption")]
        VE = 12,
        
        [Description("Workers Compensation")]
        WC = 13,
        
        [Description("Wildfire")]
        WF = 14,
        
        [Description("Winter Storm")]
        WT = 15,
        
        [Description("Wind Storm")]
        WS = 16,
        
        [Description("Collission")]
        CO = 117,
        
        [Description("Attritional")]
        AT = 17,
        
        [Description("LargeLoss")]
        LL = 18,

        [Description("Machinery Breakdown")]
        MB = 118,

        [Description("Contingent Business Interruption")]
        CBI = 119,

        [Description("NonModel")]
        NM = 19,
        
        [Description("Land Slide")]
        LS = 20,
        
        HU = 21,
        LR = 22,
        YY = 23,
    }
}
