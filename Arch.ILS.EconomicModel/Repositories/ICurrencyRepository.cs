
namespace Arch.ILS.EconomicModel
{
    public interface ICurrencyRepository
    {
        #region FX Rates

        Task<FXTable> GetFXRates();

        #endregion FX Rates
    }
}
