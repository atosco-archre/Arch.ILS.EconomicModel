
namespace Arch.ILS.EconomicModel
{
    public readonly struct PeriodCession
    {
        public PeriodCession(in DateTime startInclusive, in DateTime endInclusive, in decimal cession)
        {
            StartInclusive = startInclusive;
            EndInclusive = endInclusive;
            NetCession = cession;
        }

        public DateTime StartInclusive { get; init; }
        public DateTime EndInclusive { get; init; }
        public decimal NetCession { get; init; }
    }
}
