
using System.Text.RegularExpressions;

using Arch.ILS.Snowflake;

namespace Arch.ILS.EconomicModel.Stochastic
{
    public class MixedSnowflakeRepository : IMixedRepository
    {
        #region Variables

        private static readonly Regex _rowVersionToBigIntRegex;
        private static object _bulkLoadLayerItdMetricsLock;
        private static object _bulkLoadYeltOriginalLock;
        private static object _bulkLoadYeltRecalculatedLock;
        private static object _bulkLoadYeltConditionalLock;
        private static object _bulkLoadRetroCessionLock;
        private static object _bulkLoadRetroLayerCessionlLock;
        private readonly SnowflakeRepository _repository;

        #endregion Variables

        #region Constructors

        static MixedSnowflakeRepository()
        {
            _rowVersionToBigIntRegex = new Regex("CAST\\s*\\(\\s*(?<Alias>[^\\s]+\\.)*ROWVERSION\\s* AS \\s*BIGINT\\s*\\)", RegexOptions.IgnoreCase);
            _bulkLoadLayerItdMetricsLock = new object();
            _bulkLoadYeltOriginalLock = new object();
            _bulkLoadYeltRecalculatedLock = new object();
            _bulkLoadYeltConditionalLock = new object();
            _bulkLoadRetroCessionLock = new object();
            _bulkLoadRetroLayerCessionlLock = new object();
        }

        public MixedSnowflakeRepository(string connectionSring)
        {
            _repository = new SnowflakeRepository(connectionSring);
        }

        #endregion Constructors

        #region Methods

        public IEnumerable<LayerActualQuarterMetrics> GetRetroLayerActualITDQuarterMetrics(int retroProgramId, int acctGPeriod)
        {
            using (var reader = _repository.ExecuteReaderSql(string.Format(GET_RETRO_LAYERS_ITD_ACTUAL_QUARTERLY_METRICS_QUERY, retroProgramId, acctGPeriod)))
            {
                while (reader.Read())
                {
                    int index = 0;
                    yield return new LayerActualQuarterMetrics
                    {
                        MasterKey = reader.GetString(index),
                        MasterKeyFrom = reader.GetString(++index),
                        LayerId = reader.GetInt32(++index),
                        SubmissionId = reader.GetInt32(++index),
                        IsMultiYear = reader.GetBoolean(++index),
                        IsCancellable = reader.GetBoolean(++index),
                        UWYear = reader.GetInt32(++index),
                        AcctGPeriod = reader.GetInt32(++index),
                        Segment = reader.GetString(++index),
                        PerspectiveType =(RegisPerspectiveType)reader.GetInt32(++index),
                        Currency = reader.GetString(++index),
                        Facility = reader.GetString(++index),
                        WP = reader.GetDouble(++index),
                        WPxRP = reader.GetDouble(++index),
                        RP = reader.GetDouble(++index),
                        EP = reader.GetDouble(++index),
                        UltLoss = reader.GetDouble(++index),
                        //LimitPctUsed = reader.GetDouble(++index),
                    };
                }

                reader.Close();
            }
        }

        public IEnumerable<LayerActualMetrics> GetRetroLayerActualITDMetrics(int retroProgramId, int acctGPeriod)
        {
            using (var reader = _repository.ExecuteReaderSql(string.Format(GET_RETRO_LAYERS_ITD_ACTUAL_METRICS_QUERY, retroProgramId, acctGPeriod)))
            {
                while (reader.Read())
                {
                    int index = 0;
                    yield return new LayerActualMetrics
                    {
                        MasterKey = reader.GetString(index),
                        MasterKeyFrom = reader.GetString(++index),
                        LayerId = reader.GetInt32(++index),
                        SubmissionId = reader.GetInt32(++index),
                        IsMultiYear = reader.GetBoolean(++index),
                        IsCancellable = reader.GetBoolean(++index),
                        UWYear = reader.GetInt32(++index),
                        Segment = reader.GetString(++index),
                        PerspectiveType = (RegisPerspectiveType)reader.GetInt32(++index),
                        Currency = reader.GetString(++index),
                        Facility = reader.GetString(++index),
                        //ArchContractLimit = reader.GetDouble(++index),
                        //ArchAggLimit = reader.GetDouble(++index),
                        //Retention = reader.GetDouble(++index),
                        WP = reader.GetDouble(++index),
                        WPxRP = reader.GetDouble(++index),
                        RP = reader.GetDouble(++index),
                        UltLoss = reader.GetDouble(++index),
                        //LimitPctUsed = reader.GetDouble(++index),
                    };
                }

                reader.Close();
            }
        }

        public void AddCalculationHeader(in int calculationId, in string calculationName, in DateTime conditionalCutoffDate, in int acctGPeriod, in DateTime asAtDate, in bool useBoundFx, in string baseCurrency, in DateTime currentFXDate)
        {
            _repository.ExecuteSql($"INSERT INTO ACTUARIAL_ILS_POC.STC.CALCULATION_HEADER VALUES({calculationId}, '{calculationName}', TO_TIMESTAMP('{conditionalCutoffDate.ToString("yyyy-MM-dd HH:mm:ss")}', {acctGPeriod}, TO_TIMESTAMP('{asAtDate.ToString("yyyy-MM-dd HH:mm:ss")}', 'YYYY-MM-DD HH:MI:SS'), {useBoundFx}, '{baseCurrency}', TO_TIMESTAMP('{currentFXDate.ToString("yyyy-MM-dd HH:mm:ss")}', 'YYYY-MM-DD HH:MI:SS'), TO_TIMESTAMP('{DateTime.Now.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss")}', 'YYYY-MM-DD HH:MI:SS'))");
        }

        public void BulkLoadLayerItdMetrics(in string filePath, in string fileNameWithExtension)
        {
            lock (_bulkLoadLayerItdMetricsLock)
            {
                _repository.ExecuteSql($"PUT file://{filePath} @\"ACTUARIAL_ILS_POC\".\"STC\".ACTUARIAL_ILS_POC_STC_STAGE;");
                _repository.ExecuteSql($"COPY INTO \"ACTUARIAL_ILS_POC\".\"STC\".\"LAYER_ITD_METRICS\" FROM @\"ACTUARIAL_ILS_POC\".\"STC\".ACTUARIAL_ILS_POC_STC_STAGE FILES = ('{fileNameWithExtension}.gz')  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1) PURGE = TRUE;");
            }
        }

        public void BulkLoadOriginalYelt(in string filePath, in string fileNameWithExtension)
        {
            lock (_bulkLoadYeltOriginalLock)
            {
                _repository.ExecuteSql($"PUT file://{filePath} @\"ACTUARIAL_ILS_POC\".\"STC\".ACTUARIAL_ILS_POC_STC_STAGE;");
                _repository.ExecuteSql($"COPY INTO \"ACTUARIAL_ILS_POC\".\"STC\".\"LAYERYELT_ORIGINAL\" FROM @\"ACTUARIAL_ILS_POC\".\"STC\".ACTUARIAL_ILS_POC_STC_STAGE FILES = ('{fileNameWithExtension}.gz')  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1) PURGE = TRUE;");
            }
        }

        public void BulkLoadRecalculatedYelt(in string filePath, in string fileNameWithExtension)
        {
            lock(_bulkLoadYeltRecalculatedLock)
            {
                _repository.ExecuteSql($"PUT file://{filePath} @\"ACTUARIAL_ILS_POC\".\"STC\".ACTUARIAL_ILS_POC_STC_STAGE;");
                _repository.ExecuteSql($"COPY INTO \"ACTUARIAL_ILS_POC\".\"STC\".\"LAYERYELT_RECALCULATED\" FROM @\"ACTUARIAL_ILS_POC\".\"STC\".ACTUARIAL_ILS_POC_STC_STAGE FILES = ('{fileNameWithExtension}.gz')  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1) PURGE = TRUE;");
            }
        }

        public void BulkLoadConditionalYelt(in string filePath, in string fileNameWithExtension)
        {
            lock (_bulkLoadYeltConditionalLock)
            {
                _repository.ExecuteSql($"PUT file://{filePath} @\"ACTUARIAL_ILS_POC\".\"STC\".ACTUARIAL_ILS_POC_STC_STAGE;");
                _repository.ExecuteSql($"COPY INTO \"ACTUARIAL_ILS_POC\".\"STC\".\"LAYERYELT_CONDITIONAL\" FROM @\"ACTUARIAL_ILS_POC\".\"STC\".ACTUARIAL_ILS_POC_STC_STAGE FILES = ('{fileNameWithExtension}.gz')  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1) PURGE = TRUE;");
            }
        }

        public void BulkLoadRetroCessionMetrics(in string filePath, in string fileNameWithExtension)
        {
            lock (_bulkLoadRetroCessionLock)
            {
                _repository.ExecuteSql($"PUT file://{filePath} @\"ACTUARIAL_ILS_POC\".\"STC\".ACTUARIAL_ILS_POC_STC_STAGE;");
                _repository.ExecuteSql($"COPY INTO \"ACTUARIAL_ILS_POC\".\"STC\".\"RETRO_CESSION\" FROM @\"ACTUARIAL_ILS_POC\".\"STC\".ACTUARIAL_ILS_POC_STC_STAGE FILES = ('{fileNameWithExtension}.gz')  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1) PURGE = TRUE;");
            }
        }

        public void BulkLoadRetroLayerCessionMetrics(in string filePath, in string fileNameWithExtension)
        {
            lock (_bulkLoadRetroLayerCessionlLock)
            {
                _repository.ExecuteSql($"PUT file://{filePath} @\"ACTUARIAL_ILS_POC\".\"STC\".ACTUARIAL_ILS_POC_STC_STAGE;");
                _repository.ExecuteSql($"COPY INTO \"ACTUARIAL_ILS_POC\".\"STC\".\"RETRO_LAYER_PERIOD_CESSION\" FROM @\"ACTUARIAL_ILS_POC\".\"STC\".ACTUARIAL_ILS_POC_STC_STAGE FILES = ('{fileNameWithExtension}.gz')  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1) PURGE = TRUE;");
            }
        }

        #region Query Conversion

        private string Translate(in string sqlQuery)
            => _rowVersionToBigIntRegex.Replace(sqlQuery, new MatchEvaluator((match) => $"TO_NUMBER(CAST({match.Groups["Alias"]}ROWVERSION AS VARCHAR), 'XXXXXXXXXXXXXXXX')"));

        #endregion Query Conversion

        #endregion Methods

        #region Constants

        private const string GET_RETRO_LAYERS_ITD_ACTUAL_QUARTERLY_METRICS_QUERY = @"
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

        private const string GET_RETRO_LAYERS_ITD_ACTUAL_METRICS_QUERY = @"
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
--      ,QTD.ACCTGPERIOD		
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
    GROUP BY qtd.Masterkey, QTD.MASTERKEYFROM,X.LAYERID/*,X.StatusName*/,X.SUBMISSIONID,X.ISMULTIYEAR,X.ISCANCELLABLE, qtd.UY/*, QTD.AcctgPeriod*/, seg.Segment, qtd.PerspectiveID/*, PERSPECTIVEDESC*/, qtd.Currency, QTD.Facility;";

        #endregion Constants
    }
}
