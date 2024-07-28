
namespace Arch.ILS.EconomicModel
{
    public interface IRevoRepository
    {
        Task<Dictionary<int, Layer>> GetLayers();
        Task<Dictionary<int, RetroProgram>> GetRetroPrograms();
        Task<Dictionary<int, Portfolio>> GetPortfolios();
        Task<Dictionary<int, PortLayer>> GetPortfolioLayers();
        Task<IEnumerable<PortLayerCession>> GetPortfolioLayerCessions();
        Task<IEnumerable<PortLayerCession>> GetPortfolioLayerCessionsParallel(int partitionCount = 8);
    }
}
