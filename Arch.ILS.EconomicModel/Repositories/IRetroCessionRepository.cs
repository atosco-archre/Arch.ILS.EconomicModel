
namespace Arch.ILS.EconomicModel
{
    public interface IRetroCessionRepository
    {
        #region Retro Cession Info

        Task<IList<RetroAllocation>> GetRetroAllocations(HashSet<int> retroIdFilter = null);

        Task<IEnumerable<PortLayerCession>> GetPortfolioLayerCessions();

        Task<IEnumerable<PortLayerCession>> GetPortfolioLayerCessionsParallel(int partitionCount = 8);

        Task<PortfolioRetroCessions> GetPortfolioRetroCessionView(ResetType resetType, int partitionCount = 8);

        Task<RetroCessions> GetRetroCessionView(ResetType resetType = ResetType.RAD);

        Task<IEnumerable<RetroCession>> GetRetroResetCessions();

        Task<IEnumerable<RetroCession>> GetRetroInitialCessions();

        Task<IEnumerable<InvestorCession>> GetInvestorResetCessions();

        Task<IEnumerable<InvestorCession>> GetInvestorResetCessions(Task<IEnumerable<RetroInvestorReset>> retroInvestorsResets, Task<IEnumerable<RetroProgramReset>> retroProgramResets);

        Task<IEnumerable<InvestorCession>> GetInvestorInitialCessions();

        Task<IEnumerable<InvestorCession>> GetInvestorInitialCessions(Task<IList<RetroInvestor>> retroInvestors, Task<Dictionary<int, SPInsurer>> spInsurers, Task<Dictionary<int, RetroProgram>> retroPrograms);

        Task<IEnumerable<LayerRetroPlacement>> GetLayerRetroPlacements();

        #endregion Retro Cession Info
    }
}
