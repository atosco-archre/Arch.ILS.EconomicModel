
namespace Arch.ILS.EconomicModel.Stochastic
{
    public record class ConditionalCalculationInputBase
    {
        public int CalculationId { get; set; }
        public required string CalculationName { get; set; }
        public int RetroProgramId { get; set; }
        public DateTime ConditionalCutoffDate { get; set; }
        public ResetType ResetType { get; set; }
        public bool UseBoundFx { get; set; }
        public Currency BaseCurrency { get; set; }
        public DateTime CurrentFXDate { get; set; }
        public DateTime AsAtDate { get; set; }
        public int AcctGPeriod => GetAcctGPeriod(AsAtDate);

        public static DateTime GetQuarterEndDate(in DateTime asAtDate)
            => new DateTime((asAtDate.Month - 1) / 3 == 0 ? (asAtDate.Year - 1) : asAtDate.Year, (asAtDate.Month - 1) / 3 == 0 ? 12 : (asAtDate.Month - 1) / 3 * 3, 1).AddMonths(1).AddDays(-1);

        public static int GetAcctGPeriod(in DateTime asAtDate)
            => (asAtDate.Month - 1) / 3 == 0 ? (asAtDate.Year - 1) * 100 + 12 : asAtDate.Year * 100 + (asAtDate.Month - 1) / 3 * 3;

    }
}
