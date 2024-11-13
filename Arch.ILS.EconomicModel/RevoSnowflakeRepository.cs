
using Arch.ILS.Snowflake;
using System.Text.RegularExpressions;

namespace Arch.ILS.EconomicModel
{
    public class RevoSnowflakeRepository : RevoRepository
    {
        #region Variables

        private static readonly Regex _convertRegex;
        private static readonly Regex _rowVersionToBigIntRegex;
        private static readonly Regex _quotedColumnRegex;

        #endregion Variables

        #region Constructors

        static RevoSnowflakeRepository()
        {
            _convertRegex = new Regex("CONVERT\\s*\\((?<Type>[^,]+)\\s*,\\s*(?<Column>[^,]+)\\s*\\)", RegexOptions.IgnoreCase);
            _rowVersionToBigIntRegex = new Regex("CAST\\s*\\(\\s*ROWVERSION\\s* AS \\s*BIGINT\\s*\\)", RegexOptions.IgnoreCase);
            _quotedColumnRegex = new Regex("\\[(?<Column>[^\\]]+)\\]", RegexOptions.IgnoreCase);
        }

        public RevoSnowflakeRepository(string connectionSring)
            : base(new SnowflakeRepository(connectionSring))
        {
        }

        #endregion Constructors

        #region Methods

        #region Query Conversion

        protected override string Translate(in string sqlQuery)
        {
            string newQuery = _convertRegex.Replace(sqlQuery, new MatchEvaluator((match) => $"CAST({match.Groups["Column"]} AS {match.Groups["Type"]})"));
            newQuery = _rowVersionToBigIntRegex.Replace(newQuery, "TO_NUMBER(CAST(ROWVERSION AS VARCHAR), 'XXXXXXXXXXXXXXXX')");
            newQuery = _quotedColumnRegex.Replace(newQuery, new MatchEvaluator((match) => $@"""{match.Groups["Column"].Value.ToUpperInvariant()}"""));
            return newQuery;
        }

        #endregion Query Conversion

        #endregion Methods

        #region Constants

        private const string GET_LAYER_REINSTATEMENTS = @"SELECT ReinstatementId
     , LayerId
     , ""ORDER""
     , Quantity
     , Premium
     , Brokerage
     , TO_NUMBER(CAST(ROWVERSION AS VARCHAR), 'XXXXXXXXXXXXXXXX') AS RowVersion
  FROM dbo.Reinstatement
 WHERE IsActive = 1
   AND IsDeleted = 0";

        #endregion Constants
    }
}
