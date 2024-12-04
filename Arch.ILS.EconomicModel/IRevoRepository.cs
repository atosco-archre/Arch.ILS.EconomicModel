
namespace Arch.ILS.EconomicModel
{
    public interface IRevoRepository : ILayerRepository, ILayerLossAnalysisRepository, IRetroLayerRepository, IPortfolioRetroLayerRepository
    {
        Task<Dictionary<int, RetroProgram>> GetRetroPrograms();
        Task<Dictionary<int, Portfolio>> GetPortfolios();
        Task<Dictionary<int, PortLayer>> GetPortfolioLayers();
        Task<IEnumerable<PortLayerCession>> GetPortfolioLayerCessions();
        Task<IEnumerable<PortLayerCession>> GetPortfolioLayerCessionsParallel(int partitionCount = 8);
    }
}
