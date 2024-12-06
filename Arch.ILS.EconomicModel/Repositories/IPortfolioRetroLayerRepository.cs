
namespace Arch.ILS.EconomicModel
{
    public interface IPortfolioRetroLayerRepository
    {
        Task<IEnumerable<PortfolioRetroLayer>> GetPortfolioRetroLayers();

        Task<IEnumerable<PortfolioRetroLayer>> GetPortfolioRetroLayers(long afterRowVersion);
    }
}
