
namespace Arch.ILS.EconomicModel
{
    public record class InvestorCession(in int RetroInvestorId, in int RetroProgramResetId, in int RetroProgramId, in DateTime StartDate, in decimal InvestmentSignedAmt, in decimal TargetCollateral, in decimal InvestmentSigned) 
        : RetroCession(in RetroProgramResetId, in RetroProgramId, in StartDate, in InvestmentSignedAmt, in TargetCollateral, in InvestmentSigned);
}
