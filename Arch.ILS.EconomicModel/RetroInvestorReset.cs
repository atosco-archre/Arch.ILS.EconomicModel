
using Studio.Core;

namespace Arch.ILS.EconomicModel
{
    public class RetroInvestorReset : IRecord
    {
        [Field(0)]
        public int RetroInvestorResetId { get; set; }
        [Field(1)]
        public int RetroInvestorId { get; set; }
        [Field(2)]
        public int RetroProgramResetId { get; set; }
        [Field(3)]
        public DateTime StartDate { get; set; }
        [Field(4)]
        public decimal TargetCollateral { get; set; }
        [Field(5)]
        public decimal TargetPremium { get; set; }
        [Field(6)]
        public decimal InvestmentSignedAmt { get; set; }
        [Field(7)]
        public decimal InvestmentSigned { get; set; }
        [Field(8)]
        public long RowVersion { get; set; }
        //public DateTime CreateDate { get; set; }
        //public string CreateUser { get; set; }
        //public DateTime ModifyDate { get; set; }
        //public string ModifyUser { get; set; }
        //public bool IsActive { get; set; }
        //public bool IsDeleted { get; set; }
    }
}
