
namespace Arch.ILS.EconomicModel.Historical
{
    public class ScenarioLayerLoss
    {
        public long ScenarioId { get; set; }
        //public string EventKey { get; set; }
        public int LayerId { get; set; }
        //public int SubmissionId { get; set; }
        //public int ProgramId { get; set; }
        //public long CedentId { get; set; }
        public bool IsFHCF { get; set; }
        public decimal LAE { get; set; }
        public string LossCurrency { get; set; }
        public decimal FXRateToLayerCurrency { get; set; }
        public string LayerCurrency { get; set; }
        public decimal? GULoss { get; set; }
        //public decimal? GULossWithLAE { get; set; }
        //public decimal? LayerLoss { get; set; }
        //public decimal? GrossLoss { get; set; }
        //public decimal? NetLoss { get; set; }
        //public decimal? OccGrossLoss { get; set; }
        //public decimal? OccNetLoss { get; set; }
        //public decimal? SectionsAdjustment { get; set; }
        //public decimal? LastCumulativeOccLoss { get; set; }
        //public decimal? LastAggLoss { get; set; }
        //public decimal? OccLoss { get; set; }
        //public decimal? AggLoss { get; set; }
        //public decimal? NewAggLimit { get; set; }
        //public decimal? NewAggRetention { get; set; }
        //public decimal? NewAggLimitInLayerCurrency { get; set; }
        //public decimal? NewAggRetentionInLayerCurrency { get; set; }
        //public int CalculationStatus { get; set; }
        //public int GrossTranType { get; set; }
        //public int NetTranType { get; set; }
        //public decimal? TotalNetRetroCession { get; set; }
        //public decimal? GULossBasedOnAdjustedMKS { get; set; }
        //public decimal? GULossBasedOnMKS { get; set; }
        //public decimal? GULossOverridden { get; set; }
        //public decimal? CededReinstPremium { get; set; }
        //public decimal? GrossReinstPremium { get; set; }
        //public decimal? LimitUsed { get; set; }
        //public decimal? LossCeded { get; set; }
        //public decimal? NetReinstPremium { get; set; }
        //public decimal? OccLossCeded { get; set; }
        //public decimal? ReinstPremium { get; set; }
        //public decimal? MarketShare { get; set; }
        //public long? CompanyCode { get; set; }
        //public string RiskRegion { get; set; }
        //public string RiskZone { get; set; }
    }
}
