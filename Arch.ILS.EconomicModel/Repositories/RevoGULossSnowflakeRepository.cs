
using Arch.ILS.Snowflake;

namespace Arch.ILS.EconomicModel
{
    public class RevoGULossSnowflakeRepository : RevoGULossRepository
    {
        #region Constructor

        public RevoGULossSnowflakeRepository(string connectionString) : base(new SnowflakeRepository(connectionString))
        {
        }

        #endregion Constructor      
    }
}
