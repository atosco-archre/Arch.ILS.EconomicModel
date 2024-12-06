
namespace Arch.ILS.EconomicModel
{
    public interface IRetroCessionRepository
    {
        #region Retro Cession Info

        Task<IList<RetroAllocation>> GetRetroAllocations();

        Task<IEnumerable<PortLayerCession>> GetPortfolioLayerCessions();

        Task<IEnumerable<PortLayerCession>> GetPortfolioLayerCessionsParallel(int partitionCount = 8);

        Task<PortfolioRetroCessions> GetPortfolioRetroCessionView(ResetType resetType, int partitionCount = 8);

        Task<RetroCessions> GetRetroCessionView(ResetType resetType = ResetType.RAD);

        Task<IEnumerable<RetroCession>> GetRetroResetCessions();

        Task<IEnumerable<RetroCession>> GetRetroInitialCessions();

        Task<IEnumerable<InvestorCession>> GetInvestorResetCessions();

        Task<IEnumerable<InvestorCession>> GetInvestorInitialCessions();

        Task<IEnumerable<LayerRetroPlacement>> GetLayerRetroPlacements();

        #endregion Retro Cession Info
    }
}
