
namespace Arch.ILS.EconomicModel
{
    public interface IRevoGULossRepository
    {
        #region Methods

        Task<IEnumerable<RevoGUYeltEntry>> GetRevoGUYelt(int guAnalysisId);

        Task<IEnumerable<RevoGUYeltEntry>> GetRevoGUYelt(in int guLossAnalysisId, int partitionCount);

        #endregion Methods
    }
}
