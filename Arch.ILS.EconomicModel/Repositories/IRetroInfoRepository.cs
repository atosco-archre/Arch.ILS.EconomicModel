
namespace Arch.ILS.EconomicModel
{
    public interface IRetroInfoRepository
    {
        #region Retro Info

        Task<Dictionary<int, RetroProgram>> GetRetroPrograms();

        Task<IEnumerable<RetroInvestorReset>> GetRetroInvestorResets();

        Task<IEnumerable<RetroProgramReset>> GetRetroProgramResets();

        Task<Dictionary<int, SPInsurer>> GetSPInsurers();

        Task<IList<RetroInvestor>> GetRetroInvestors();

        Task<IEnumerable<RetroZone>> GetRetroZones();

        Task<IDictionary<int, RetroProfile>> GetRetroProfiles();

        #endregion Retro Info
    }
}
