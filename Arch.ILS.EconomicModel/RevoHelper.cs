
namespace Arch.ILS.EconomicModel
{
    public static class RevoHelper
    {
        public static decimal GetFxRate(bool useBoundFx, DateTime currentFxDate, string baseCurrency, Submission submission, LayerDetail layerDetail, FXTable fxTable)
        {
            if (submission.Currency == baseCurrency)
                return decimal.One;

            if (useBoundFx)
            {
                return (layerDetail.BoundFXRate == null || layerDetail.BoundFXDate == null || layerDetail.Status != ContractStatus.Bound) ?
                    (baseCurrency == submission.BaseCurrency ? decimal.One : fxTable.GetRate(submission.FXDate, baseCurrency, submission.BaseCurrency)) * submission.FXRate :
                    (baseCurrency == submission.BaseCurrency ? decimal.One : fxTable.GetRate((DateTime)layerDetail.BoundFXDate, baseCurrency, submission.BaseCurrency)) * (decimal)layerDetail.BoundFXRate;
            }
            else
                return fxTable.GetRate(currentFxDate, baseCurrency, submission.Currency);
        }
    }
}
