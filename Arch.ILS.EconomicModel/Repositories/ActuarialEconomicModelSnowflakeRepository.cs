﻿
using System.Text.RegularExpressions;

using Arch.ILS.Snowflake;

namespace Arch.ILS.EconomicModel
{
    public class ActuarialEconomicModelSnowflakeRepository
    {
        #region Variables

        private static readonly Regex _rowVersionToBigIntRegex;
        private readonly SnowflakeRepository _snowflakeRepository;

        #endregion Variables

        #region Constructors

        static ActuarialEconomicModelSnowflakeRepository()
        {
            _rowVersionToBigIntRegex = new Regex("CAST\\s*\\(\\s*(?<Alias>[^\\s]+\\.)*ROWVERSION\\s* AS \\s*BIGINT\\s*\\)", RegexOptions.IgnoreCase);
        }

        public ActuarialEconomicModelSnowflakeRepository(string connectionSring)
        {
            _snowflakeRepository = new SnowflakeRepository(connectionSring);
        }

        #endregion Constructors

        #region Methods

        #region Query Conversion

        private string Translate(in string sqlQuery)
            => _rowVersionToBigIntRegex.Replace(sqlQuery, new MatchEvaluator((match) => $"TO_NUMBER(CAST({match.Groups["Alias"]}ROWVERSION AS VARCHAR), 'XXXXXXXXXXXXXXXX')"));

        #endregion Query Conversion

        #endregion Methods

        #region Constants

        #endregion Constants
    }
}