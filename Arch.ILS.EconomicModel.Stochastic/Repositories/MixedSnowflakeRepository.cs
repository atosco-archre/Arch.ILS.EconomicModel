
using System.Text.RegularExpressions;

using Arch.ILS.Snowflake;

namespace Arch.ILS.EconomicModel.Stochastic
{
    public class MixedSnowflakeRepository : IMixedRepository
    {
        #region Variables

        private static readonly Regex _rowVersionToBigIntRegex;
        private readonly SnowflakeRepository _repository;
        private object _bulkCopyLock;

        #endregion Variables

        #region Constructors

        static MixedSnowflakeRepository()
        {
            _rowVersionToBigIntRegex = new Regex("CAST\\s*\\(\\s*(?<Alias>[^\\s]+\\.)*ROWVERSION\\s* AS \\s*BIGINT\\s*\\)", RegexOptions.IgnoreCase);
        }

        public MixedSnowflakeRepository(string connectionSring)
        {
            _repository = new SnowflakeRepository(connectionSring);
            _bulkCopyLock = new object();
        }

        #endregion Constructors

        #region Methods

        public IEnumerable<LayerActualMetrics> GetRetroLayerActualITDMetrics(int retroProgramId, int acctGPeriod)
        {
            using (var reader = _repository.ExecuteReaderSql(string.Format(GET_RETRO_LAYERS_YTDA_ACTUAL_METRICS_QUERY, retroProgramId, acctGPeriod)))
            {
                while (reader.Read())
                {
                    yield return new LayerActualMetrics
                    {
                        MasterKey = reader.GetString(0),
                        MasterKeyFrom = reader.GetString(1),
                        LayerId = reader.GetInt32(2),
                        SubmissionId = reader.GetInt32(3),
                        IsMultiYear = reader.GetBoolean(4),
                        IsCancellable = reader.GetBoolean(5),
                        UWYear = reader.GetInt32(6),
                        AcctGPeriod = reader.GetInt32(7),
                        Segment = reader.GetString(8),
                        PerspectiveType =(RegisPerspectiveType)reader.GetInt32(9),
                        Currency = reader.GetString(10),
                        Facility = reader.GetString(11),
                        WrittenPremium = reader.GetDouble(12),
                        WrittenPremiumxReinstatementPremium = reader.GetDouble(13),
                        ReinstatementPremium = reader.GetDouble(14),
                        EarnedPremium = reader.GetDouble(15),
                        UltimateLoss = reader.GetDouble(16),
                    };
                }

                reader.Close();
            }
        }

        public void BulkCopyRecalculatedYelt(string filePath)
        {
            lock(_bulkCopyLock)
            {
                //_repository.ExecuteSql($"PUT file://{filePath} @\"ACTUARIAL_ILS_POC\".\"STC\".ACTUARIAL_ILS_POC_STC_STAGE;");
                //_repository.ExecuteSql($"COPY INTO \"ACTUARIAL_ILS_POC\".\"STC\".\"LAYERYELT_RECALCULATED\" FROM @\"ACTUARIAL_ILS_POC\".\"STC\".ACTUARIAL_ILS_POC_STC_STAGE FILE_FORMAT = (TYPE = CSV) PARSE_HEADER = TRUE;");
            }
        }
        
        #region Query Conversion

        private string Translate(in string sqlQuery)
            => _rowVersionToBigIntRegex.Replace(sqlQuery, new MatchEvaluator((match) => $"TO_NUMBER(CAST({match.Groups["Alias"]}ROWVERSION AS VARCHAR), 'XXXXXXXXXXXXXXXX')"));

        #endregion Query Conversion

        #endregion Methods

        #region Constants

        private const string GET_RETRO_LAYERS_YTDA_ACTUAL_METRICS_QUERY = @"
WITH 
--ContractStatus
--AS
--(
--  SELECT apppref.VALUE AS StatusCode
--       , apppref.NAME AS StatusName
--    FROM REVO_BERMUDA.dbo.appsection
--   INNER JOIN REVO_BERMUDA.dbo.apppref
--      ON apppref.appsectionid = appsection.appsectionid
--   WHERE appsection.APPSECTIONID = 14
--),
 X
AS
(
SELECT DISTINCT RA.LAYERID
     , L.REGISMKEY			
     , L.SUBMISSIONID		
     , S.ISMULTIYEAR		
     , S.ISCANCELLABLE	
     , L.STATUS
     --, CS.StatusName
  FROM REVO_BERMUDA.DBO.RETROALLOCATION RA
 INNER JOIN REVO_BERMUDA.DBO.RETROINVESTOR RI
    ON RI.RETROINVESTORID = RA.RETROINVESTORID
 INNER JOIN REVO_BERMUDA.DBO.SPINSURER SPI
    ON SPI.SPINSURERID = RI.SPINSURERID 
 INNER JOIN REVO_BERMUDA.DBO.LAYER L
    ON L.LAYERID = RA.LAYERID
 INNER JOIN REVO_BERMUDA.DBO.SUBMISSION S	
    ON S.SUBMISSIONID = L.SUBMISSIONID	
 --INNER JOIN CONTRACTSTATUS CS
 --   ON CS.StatusCode = L.STATUS    
 WHERE SPI.RETROPROGRAMID = {0}
   AND RA.ISACTIVE
   AND NOT(RA.ISDELETED)
   AND RI.ISACTIVE
   AND NOT(RI.ISDELETED)
   AND SPI.ISACTIVE
   AND NOT(SPI.ISDELETED)  
   AND CESSIONGROSS > 0
   AND REGISMKEY IS NOT NULL 
   AND REGISMKEY != ''
)
    SELECT		
        QTD.MASTERKEY		
       ,QTD.MASTERKEYFROM		
       ,X.LAYERID
 --      ,X.StatusName
       ,X.SUBMISSIONID		
       ,X.ISMULTIYEAR		
       ,X.ISCANCELLABLE		
       ,QTD.UY		
       ,QTD.ACCTGPERIOD		
       ,SEG.SEGMENT		
       ,QTD.PERSPECTIVEID		
-- ,CASE QTD.PERSPECTIVEID WHEN 1 THEN 'Assumed' WHEN 2 THEN 'Ceded' WHEN 3 THEN 'Bermuda Cession' WHEN 4 THEN 'LPT Cession' END AS PERSPECTIVEDESC		
       ,QTD.CURRENCY		
       ,QTD.FACILITY		
       ,SUM(QTD.WP) AS ""WRITTEN PREMIUM""		
       ,SUM(QTD.WP - QTD.RIP) AS ""WRITTEN PREMIUM excl. REINSTATEMENT PREMIUM""		
       ,SUM(QTD.RIP) AS ""REINSTATEMENT PREMIUM""		
       ,SUM(QTD.EP) AS ""EARNED PREMIUM""		
       ,SUM(QTD.LOSS_ULT) AS ""ULTIMATE LOSS""	
    FROM (SELECT DISTINCT FACILITY, SEGMENT FROM ARCHDM.DBO.TBLSEGMENT) SEG		
    INNER JOIN ARCHDM.DBO.TBLIBNR_QTD QTD ON SEG.FACILITY = QTD.FACILITY 	
    INNER JOIN X ON X.REGISMKEY = QTD.MASTERKEY
    WHERE ((QTD.RC=1) AND QTD.CURRENCY = 'USD' AND ((QTD.AcctgPeriod)<={1}) AND (QTD.PerspectiveID IN (1,2))) 		
    GROUP BY qtd.Masterkey, QTD.MASTERKEYFROM,X.LAYERID/*,X.StatusName*/,X.SUBMISSIONID,X.ISMULTIYEAR,X.ISCANCELLABLE, qtd.UY, QTD.AcctgPeriod, seg.Segment, qtd.PerspectiveID/*, PERSPECTIVEDESC*/, qtd.Currency, QTD.Facility;";

        #endregion Constants
    }
}
