
using System.ComponentModel;

namespace Arch.ILS.EconomicModel
{
    public enum LimitBasis
    {
        [Description("Not Applicable")]
        NotApplicable = 0,

        [Description("Aggregate")]
        Aggregate = 1,

        [Description("CAT & Risk")]
        CAT_and_Risk = 2,

        [Description("Per Occurrence")]
        PerOccurrence = 3,

        [Description("Per Risk")]
        PerRisk = 4,

        [Description("Quota Share")]
        QuotaShare = 5,

        [Description("Stop Loss")]
        StopLoss = 6,

        [Description("Non CAT Quota Share")]
        NonCATQuotaShare = 7,

        [Description("Other")]
        Other = 9,

        [Description("Occurrence Agg")]
        OccurrenceAgg = 10
    }
}
