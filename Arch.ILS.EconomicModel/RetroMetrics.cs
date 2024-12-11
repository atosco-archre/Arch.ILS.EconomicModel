
namespace Arch.ILS.EconomicModel
{
    public record class RetroMetrics
    {
        public RetroMetrics(int retroProgramid)
        {
            RetroProgramId = retroProgramid;
        }

        public int RetroProgramId { get; }
        public byte RetroLevel { get; set; }
        public RetroProgramType RetroProgramType { get; set; }
        public DateTime RetroInception { get; set; }
        public DateTime RetroExpiration { get; set; }
        public decimal SubjectPremium { get; set; }
        public decimal SubjectPremiumPlaced{ get; set; }
        public decimal CededPremium { get; set; }
        public IDictionary<DateTime, LimitMetrics> DateLimits { get; set; }
    }
}
