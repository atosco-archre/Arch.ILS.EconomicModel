
namespace Arch.ILS.EconomicModel
{
    public sealed class FXTable
    {
        private readonly Dictionary<DateTime, Dictionary<string, Dictionary<string, decimal>>> _fxRates;
        private readonly SortedSet<DateTime> _fxDates;

        public FXTable()
        {
            _fxRates = new Dictionary<DateTime, Dictionary<string, Dictionary<string, decimal>>>();
            _fxDates = new SortedSet<DateTime>();
        }

        public void AddRate(FXRate fxRate)
        {
            if(!_fxRates.TryGetValue(fxRate.FXDate, out var baseCurrenciesRates))
            {
                baseCurrenciesRates = new Dictionary<string, Dictionary<string, decimal>>();
                _fxRates[fxRate.FXDate] = baseCurrenciesRates;
                _fxDates.Add(fxRate.FXDate);
            }

            if (!baseCurrenciesRates.TryGetValue(fxRate.BaseCurrency, out var rates))
            {
                rates = new Dictionary<string, decimal>();
                baseCurrenciesRates[fxRate.BaseCurrency] = rates;
            }

            rates[fxRate.Currency] = fxRate.Rate;
        }

        public DateTime GetClosestFxDate(DateTime fxDate)
        {
            return _fxDates.Where(d => d <= fxDate).Last();
        }

        public decimal GetRate(DateTime fxDate, string baseCurrency, string currency)
        {
            if (!_fxRates.TryGetValue(fxDate, out var baseCurrenciesRates))
            {
                fxDate = GetClosestFxDate(fxDate);
                baseCurrenciesRates = _fxRates[fxDate];
            }

            if(baseCurrenciesRates.TryGetValue(baseCurrency, out var rates)
                && rates.TryGetValue(currency, out decimal rate))
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
