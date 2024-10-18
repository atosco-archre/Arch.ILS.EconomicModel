
namespace Arch.ILS.EconomicModel
{
    public sealed class FXTable
    {
        private readonly Dictionary<DateTime, Dictionary<string, Dictionary<string, decimal>>> _fxRates;

        public FXTable()
        {
            _fxRates = new Dictionary<DateTime, Dictionary<string, Dictionary<string, decimal>>>();
        }

        public void AddRate(FXRate fxRate)
        {
            if(!_fxRates.TryGetValue(fxRate.FXDate, out var baseCurrenciesRates))
            {
                baseCurrenciesRates = new Dictionary<string, Dictionary<string, decimal>>();
                _fxRates[fxRate.FXDate] = baseCurrenciesRates;
            }

            if (!baseCurrenciesRates.TryGetValue(fxRate.BaseCurrency, out var rates))
            {
                rates = new Dictionary<string, decimal>();
                baseCurrenciesRates[fxRate.BaseCurrency] = rates;
            }

            rates[fxRate.Currency] = fxRate.Rate;
        }

        public decimal GetRate(DateTime fxDate, string baseCurrency, string currency)
        {
            var rates = _fxRates[fxDate][baseCurrency];
            if(rates.TryGetValue(currency, out decimal rate))
                return rate;
            
            foreach(var baseCurrencyRates in _fxRates[fxDate])
            {
                if(baseCurrencyRates.Value.TryGetValue(currency, out decimal currencyRate)
                    && baseCurrencyRates.Value.TryGetValue(baseCurrency, out decimal baseRate)) 
                    return currencyRate / baseRate;
            }

            throw new Exception("Exchange Rate not found.");
        }
    }
}
