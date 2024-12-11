
namespace Arch.ILS.EconomicModel.Stochastic
{
    public class LayerActualMetrics
    {
        public string MasterKey { get; set; }
        public string MasterKeyFrom { get; set; }
        public int LayerId { get; set; }
        public int SubmissionId { get; set; }
        public bool IsMultiYear { get; set; }
        public bool IsCancellable { get; set; }
        public int UWYear { get; set; }
        public int AcctGPeriod { get; set; }
        public string Segment { get; set; }
        public RegisPerspectiveType PerspectiveType { get; set; }
        public string Currency { get; set; }
        public string Facility { get; set; }
        public double WrittenPremium { get; set; }
        public double WrittenPremiumxReinstatementPremium { get; set; }
        public double ReinstatementPremium { get; set; }
        public double EarnedPremium { get; set; }
        public double UltimateLoss{ get; set; }

    }
}
