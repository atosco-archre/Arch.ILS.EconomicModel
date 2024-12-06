
using Arch.ILS.Snowflake;

namespace Arch.ILS.EconomicModel
{
    public class RevoLayerLossSnowflakeRepository : RevoLayerLossRepository
    {
        #region Constructor

        public RevoLayerLossSnowflakeRepository(string connectionString) : base(new SnowflakeRepository(connectionString))
        {
        }

        #endregion Constructor      
    }
}
