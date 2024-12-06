
namespace Arch.ILS.EconomicModel
{
    public interface IPortfolioInfoRepository
    {
        #region Portfolio Info

        Task<Dictionary<int, Portfolio>> GetPortfolios();

        Task<Dictionary<int, PortLayer>> GetPortfolioLayers();

        #endregion Portfolio Info
    }
}
