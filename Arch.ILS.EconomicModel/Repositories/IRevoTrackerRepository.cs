
namespace Arch.ILS.EconomicModel.Repositories
{
    public interface IRevoTrackerRepository
    {
        long GetLatestRowVersion(RevoDataTable revoDataTable);
    }
}
