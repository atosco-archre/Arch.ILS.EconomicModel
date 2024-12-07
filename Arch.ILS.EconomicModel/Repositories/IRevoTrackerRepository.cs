
namespace Arch.ILS.EconomicModel.Repositories
{
    public interface IRevoTrackerRepository
    {
        Task<long> GetLatestRowVersion(RevoDataTable revoDataTable);
    }
}
