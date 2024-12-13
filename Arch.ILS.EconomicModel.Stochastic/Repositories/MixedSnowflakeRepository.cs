
using System.Data;
using System.Text.RegularExpressions;

using Arch.ILS.Snowflake;

namespace Arch.ILS.EconomicModel.Stochastic
{
    public class MixedSnowflakeRepository : IMixedRepository
    {
        #region Constants

        private const string DATABASE_NAME = "ACTUARIAL_ILS_POC";
        private const string SCHEMA_NAME = "STC";
        private const string DATABASE_STAGE_NAME = "ACTUARIAL_ILS_POC_STC_STAGE";
        private const string TABLE_CALCULATION_HEADER = "CALCULATION_HEADER";
        private const string TABLE_LAYER_ITD_METRICS = "LAYER_ITD_METRICS";
        private const string TABLE_LAYERYELT_ORIGINAL = "LAYERYELT_ORIGINAL";
        private const string TABLE_LAYERYELT_RECALCULATED = "LAYERYELT_RECALCULATED";
        private const string TABLE_LAYERYELT_CONDITIONAL = "LAYERYELT_CONDITIONAL";
        private const string TABLE_RETRO_CESSION = "RETRO_CESSION";
        private const string TABLE_RETRO_LAYER_PERIOD_CESSION = "RETRO_LAYER_PERIOD_CESSION";       

        #endregion Constants

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

        public int AddCalculationHeader(ConditionalCalculationInputBase input)
        {
            input.CalculationId = GetNextCalculationHeader();
            _repository.ExecuteSql($"INSERT INTO {DATABASE_NAME}.{SCHEMA_NAME}.{TABLE_CALCULATION_HEADER} VALUES({input.CalculationId}, '{input.CalculationName}', TO_TIMESTAMP('{input.ConditionalCutoffDate.ToString("yyyy-MM-dd HH:mm:ss")}', 'YYYY-MM-DD HH:MI:SS'), {input.AcctGPeriod}, TO_TIMESTAMP('{input.AsAtDate.ToString("yyyy-MM-dd HH:mm:ss")}', 'YYYY-MM-DD HH:MI:SS'), {input.UseBoundFx}, '{input.BaseCurrency.ToString()}', TO_TIMESTAMP('{input.CurrentFXDate.ToString("yyyy-MM-dd HH:mm:ss")}', 'YYYY-MM-DD HH:MI:SS'), TO_TIMESTAMP('{DateTime.Now.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss")}', 'YYYY-MM-DD HH:MI:SS'))");
            return input.CalculationId;
        }

        private int GetNextCalculationHeader()
        {
            return 1 + (((int?)_repository.ExecuteScalar($"SELECT MAX(CALCULATIONID) FROM {DATABASE_NAME}.{SCHEMA_NAME}.{TABLE_CALCULATION_HEADER};")) ?? 0);
        }

        public void BulkLoadLayerItdMetrics(in int calculationId, in string filePath, in string fileNameWithExtension)
        {
            if (LayerItdMetricsExists(calculationId))
                throw new Exception($"Attempt to insert entries into {TABLE_LAYER_ITD_METRICS} table for already imported CalculationId {calculationId}.");
            BulkLoadCsv(filePath, fileNameWithExtension, TABLE_LAYER_ITD_METRICS, _bulkLoadLayerItdMetricsLock);
        }

        private bool LayerItdMetricsExists(in int calculationId)
        {
            using (IDataReader reader = _repository.ExecuteReaderSql($"SELECT TOP 1 TRUE FROM {DATABASE_NAME}.{SCHEMA_NAME}.{TABLE_LAYER_ITD_METRICS} WHERE CALCULATIONID = {calculationId};"))
            {
                return reader.Read();
            }
        }

        public void BulkLoadOriginalYelt(in int calculationId, in int layerId, in RevoLossViewType lossView, in string filePath, in string fileNameWithExtension)
        {
            if (YeltExists(in calculationId, in layerId, in lossView, TABLE_LAYERYELT_ORIGINAL))
                throw new Exception($"Attempt to insert entries into {TABLE_LAYERYELT_ORIGINAL} table for already imported CalculationId {calculationId} - LayerId {layerId} - LossView {lossView}.");
            BulkLoadCsv(filePath, fileNameWithExtension, TABLE_LAYERYELT_ORIGINAL, _bulkLoadYeltOriginalLock);
        }

        public void BulkLoadRecalculatedYelt(in int calculationId, in int layerId, in RevoLossViewType lossView, in string filePath, in string fileNameWithExtension)
        {
            if (YeltExists(in calculationId, in layerId, in lossView, TABLE_LAYERYELT_RECALCULATED))
                throw new Exception($"Attempt to insert entries into {TABLE_LAYERYELT_RECALCULATED} table for already imported CalculationId {calculationId} - LayerId {layerId} - LossView {lossView}.");
            BulkLoadCsv(filePath, fileNameWithExtension, TABLE_LAYERYELT_RECALCULATED, _bulkLoadYeltRecalculatedLock);
        }

        public void BulkLoadConditionalYelt(in int calculationId, in int layerId, in RevoLossViewType lossView, in string filePath, in string fileNameWithExtension)
        {
            if (YeltExists(in calculationId, in layerId, in lossView, TABLE_LAYERYELT_CONDITIONAL))
                throw new Exception($"Attempt to insert entries into {TABLE_LAYERYELT_CONDITIONAL} table for already imported CalculationId {calculationId} - LayerId {layerId} - LossView {lossView}.");
            BulkLoadCsv(filePath, fileNameWithExtension, TABLE_LAYERYELT_CONDITIONAL, _bulkLoadYeltConditionalLock);
        }

        private bool YeltExists(in int calculationId, in int layerId, in RevoLossViewType lossView, string tableName)
        {
            using (IDataReader reader = _repository.ExecuteReaderSql($"SELECT TOP 1 TRUE FROM {DATABASE_NAME}.{SCHEMA_NAME}.{tableName} WHERE CALCULATIONID = {calculationId} AND LAYERID = {layerId} AND LOSSVIEW = '{lossView.ToString()}';"))
            {
                return reader.Read();
            }
        }

        public void BulkLoadRetroCessionMetrics(in int calculationId, in string filePath, in string fileNameWithExtension)
        {
            if (RetroCessionMetricsExists(calculationId))
                throw new Exception($"Attempt to insert entries into {TABLE_RETRO_CESSION} table for already imported CalculationId {calculationId}.");
            BulkLoadCsv(filePath, fileNameWithExtension, TABLE_RETRO_CESSION, _bulkLoadRetroCessionLock);
        }

        private bool RetroCessionMetricsExists(in int calculationId)
        {
            using (IDataReader reader = _repository.ExecuteReaderSql($"SELECT TOP 1 TRUE FROM {DATABASE_NAME}.{SCHEMA_NAME}.{TABLE_RETRO_CESSION} WHERE CALCULATIONID = {calculationId};"))
            {
                return reader.Read();
            }
        }

        public void BulkLoadRetroLayerCessionMetrics(in int calculationId, in string filePath, in string fileNameWithExtension)
        {
            if (RetroLayerCessionMetricsExists(calculationId))
                throw new Exception($"Attempt to insert entries into {TABLE_RETRO_LAYER_PERIOD_CESSION} table for already imported CalculationId {calculationId}.");
            BulkLoadCsv(filePath, fileNameWithExtension, TABLE_RETRO_LAYER_PERIOD_CESSION, _bulkLoadRetroLayerCessionlLock);
        }

        private bool RetroLayerCessionMetricsExists(in int calculationId)
        {
            using (IDataReader reader = _repository.ExecuteReaderSql($"SELECT TOP 1 TRUE FROM {DATABASE_NAME}.{SCHEMA_NAME}.{TABLE_RETRO_LAYER_PERIOD_CESSION} WHERE CALCULATIONID = {calculationId};"))
            {
                return reader.Read();
            }
        }

        public void BulkLoadCsv(in string filePath, in string fileNameWithExtension, in string tableName, in object tableLock)
        {
            lock (tableLock)
            {
                _repository.ExecuteSql($"PUT file://{filePath} @\"{DATABASE_NAME}\".\"{SCHEMA_NAME}\".{DATABASE_STAGE_NAME};");
                _repository.ExecuteSql($"COPY INTO \"{DATABASE_NAME}\".\"{SCHEMA_NAME}\".\"{tableName}\" FROM @\"{DATABASE_NAME}\".\"{SCHEMA_NAME}\".{DATABASE_STAGE_NAME} = ('{fileNameWithExtension}.gz')  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1) PURGE = TRUE;");
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
