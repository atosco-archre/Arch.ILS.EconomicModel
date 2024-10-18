
using System.ComponentModel;

namespace Arch.ILS.EconomicModel
{
    public enum ContractStatus : byte
    {
        [Description("")]
         None = 0,

        [Description("Pending")]
        Pending = 1,

        [Description("Quoted")]
        Quoted = 2,

        [Description("Wait FOT")]
        WaitFOT = 3,

        [Description("Authorized")]
        Authorized = 4,

        [Description("Bound")]
        Bound = 10,

        [Description("Cancelled")]
        Cancelled = 12,

        [Description("Declined")]
        Declined = 20,

        [Description("NTU")]
        NTU = 21,

        [Description("Signed")]
        Signed = 22,

        [Description("FOTs")]
        FOTs = 23,

        [Description("Withdrawn")]
        Withdrawn = 24,

        [Description("Quote Ready")]
        QuoteReady = 26,

        [Description("Auth Ready")]
        AuthReady = 27,

        [Description("Not in Scope")]
        NotinScope = 30,

        [Description("Sign Ready")]
        SignReady = 31,

        [Description("Bind Ready")]
        BindReady = 32,

        [Description("Bind Requested")]
        BindRequested = 33,

        [Description("Quote Requested")]
        QuoteRequested = 34,

        [Description("Run Off")]
        RunOff = 35,

        [Description("Budget")]
        Budget = 36,

    }
}
