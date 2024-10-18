
namespace Arch.ILS.EconomicModel
{
    public record class RetroCession(in int RetroProgramResetId, in int RetroProgramId, in DateTime StartDate, in decimal InvestmentSignedAmt, in decimal TargetCollateral, in decimal InvestmentSigned)
    {
        public decimal CessionBeforePlacement => TargetCollateral == 0 ? InvestmentSigned : InvestmentSignedAmt / TargetCollateral;
    }
}
