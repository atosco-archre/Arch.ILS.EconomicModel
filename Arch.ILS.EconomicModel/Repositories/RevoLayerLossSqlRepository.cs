
using Studio.Core.Sql;

namespace Arch.ILS.EconomicModel
{
    public class RevoLayerLossSqlRepository : RevoLayerLossRepository
    {
        #region Constructor

        public RevoLayerLossSqlRepository(string connectionString) : base(new SqlRepository(connectionString))
        {
        }

        #endregion Constructor      
    }
}
