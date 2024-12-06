
using Studio.Core.Sql;

namespace Arch.ILS.EconomicModel
{
    public class RevoSqlRepository : RevoRepository
    {
        #region Constructors

        public RevoSqlRepository(string connectionSring)
            : base(new SqlRepository(connectionSring))
        {
        }

        #endregion Constructors

        #region Methods

        #endregion Methods

        #region Constants

        private const string GET_LAYER_REINSTATEMENTS = @"SELECT ReinstatementId
     , LayerId
     , [ORDER]
     , Quantity
     , Premium
     , Brokerage
     , CONVERT(BIGINT, RowVersion) AS RowVersion
  FROM dbo.Reinstatement
 WHERE IsActive = 1
   AND IsDeleted = 0";

        #endregion Constants
    }
}
