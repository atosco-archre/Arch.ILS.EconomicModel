
using Studio.Core.Sql;

namespace Arch.ILS.EconomicModel
{
    public class RevoGULossSqlRepository : RevoGULossRepository
    {
        #region Constructor

        public RevoGULossSqlRepository(string connectionString) : base(new SqlRepository(connectionString))
        {
        }

        #endregion Constructor      
    }
}
