
namespace Arch.ILS.EconomicModel
{
    public class RetroInvestor
    {
        public int RetroInvestorId { get; set; }
        public int SPInsurerId { get; set; }
        //public string Name { get; set; }
        public int Status { get; set; }
        public decimal TargetCollateral { get; set; }
        public decimal NotionalCollateral { get; set; }
        public decimal InvestmentEstimated { get; set; }
        public decimal InvestmentAuth { get; set; }
        public decimal InvestmentSigned { get; set; }
        public decimal InvestmentEstimatedAmt { get; set; }
        public decimal InvestmentAuthAmt { get; set; }
        public decimal InvestmentSignedAmt { get; set; }
        public string ExcludedFacilities { get; set; }
        public string ExcludedLayerSubNos { get; set; }
        public string ExcludedDomiciles { get; set; }
        public bool IsFundsWithheld { get; set; }
        public int RetroCommissionId { get; set; }
        //public DateTime CreateDate { get; set; }
        //public string CreateUser { get; set; }
        //public DateTime ModifyDate { get; set; }
        //public string ModifyUser { get; set; }
        //public bool IsActive { get; set; }
        //public bool IsDeleted { get; set; }
        //public string RuleDefs { get; set; }
        public long RowVersion { get; set; }
        public string ExcludedLayerIds { get; set; }
        public decimal TargetPremium { get; set; }
        public decimal Override { get; set; }
        public decimal ManagementFee { get; set; }
        public decimal ProfitComm { get; set; }
        public decimal PerformanceFee { get; set; }
        public decimal RHOE { get; set; }
        public decimal HurdleRate { get; set; }
        public bool IsPortIn { get; set; }
        public bool IsPortOut { get; set; }
        public int RetroBufferType { get; set; }
        public decimal CessionCapBufferPct { get; set; }
        public string RetroValuesToBuffer { get; set; }
        public int ExcludedContractType { get; set; }
    }
}
