
using System.Text.RegularExpressions;

using Arch.ILS.Snowflake;

namespace Arch.ILS.EconomicModel.Stochastic
{
    public class ActuarialStochasticSnowflakeRepository : IActuarialStochasticRepository
    {
        #region Variables

        private static readonly Regex _rowVersionToBigIntRegex;
        private readonly SnowflakeRepository _snowflakeRepository;

        #endregion Variables

        #region Constructors

        static ActuarialStochasticSnowflakeRepository()
        {
            _rowVersionToBigIntRegex = new Regex("CAST\\s*\\(\\s*(?<Alias>[^\\s]+\\.)*ROWVERSION\\s* AS \\s*BIGINT\\s*\\)", RegexOptions.IgnoreCase);
        }

        public ActuarialStochasticSnowflakeRepository(string connectionSring)
        {
            _snowflakeRepository = new SnowflakeRepository(connectionSring);
        }

        #endregion Constructors

        #region Methods

        #region Stc Schema

        public void CreateStcSchemaIfNotExists()
        {
            _snowflakeRepository.ExecuteSql(CREATE_STC_SCHEMA);
        }

        #endregion Stc Schema

        #region Query Conversion

            private string Translate(in string sqlQuery)
            => _rowVersionToBigIntRegex.Replace(sqlQuery, new MatchEvaluator((match) => $"TO_NUMBER(CAST({match.Groups["Alias"]}ROWVERSION AS VARCHAR), 'XXXXXXXXXXXXXXXX')"));

        #endregion Query Conversion

        #endregion Methods

        #region Constants

        private const string CREATE_STC_SCHEMA = @"CREATE SCHEMA IF NOT EXISTS STC;";

        #endregion Constants
    }
}
