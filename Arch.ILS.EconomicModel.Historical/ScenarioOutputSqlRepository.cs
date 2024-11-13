
using System.Data;
using System.Transactions;
using LargeLoss.LayeringService.Client;
using Studio.Core;
using Studio.Core.Sql;

namespace Arch.ILS.EconomicModel.Historical
{
    public partial class ScenarioSqlRepository : SqlRepository
    {
        #region Constants

        #region Scenario Layer Reinstatement

        private const string SCENARIO_LAYER_REINSTATEMENT = "dbo.ScenarioLayerReinstatement";
        private const string CREATE_SCENARIO_LAYER_REINSTATEMENT_TABLE = @"IF(OBJECT_ID('{0}') IS NULL)
CREATE TABLE {0}
(
  ScenarioId BIGINT NOT NULL
, LayerId BIGINT NOT NULL
, ReinstatementId INT NOT NULL
, ReinstatementOrder INT NOT NULL
, Quantity FLOAT NOT NULL
, PremiumShare DECIMAL(18, 10) NOT NULL
, BrokeragePercentage DECIMAL(18, 10) NOT NULL
, RowVersion BINARY(8) NOT NULL
, CONSTRAINT pk_dbo_ScenarioLayerReinstatement PRIMARY KEY CLUSTERED(ScenarioId ASC, LayerId ASC, ReinstatementId ASC)
, CONSTRAINT uk_dbo_ScenarioLayerReinstatement_ScenarioId_LayerId_ReinstatementOrder UNIQUE(ScenarioId ASC, LayerId ASC, ReinstatementOrder ASC)
, CONSTRAINT fk_dbo_ScenarioLayerReinstatement_ScenarioId_LayerId_dbo_ScenarioLayer_ScenarioId_LayerId FOREIGN KEY (ScenarioId, LayerId) REFERENCES dbo.ScenarioLayer(ScenarioId, LayerId)
);";

        #endregion Scenario Layer Reinstatement

        #region Scenario Layer Loss Aggregate

        private const string SCENARIO_LAYER_LOSS_AGGREGATE_HEADER = "dbo.ScenarioLayerLossAggregateHeader";
        private const string SCENARIO_LAYER_LOSS_AGGREGATE = "dbo.ScenarioLayerLossAggregate";
        private const string ADD_SCENARIO_LAYER_LOSS_AGGREGATE_HEADER_PROCEDURE = "dbo.AddScenarioLayerLossAggregateHeader";

        private const string CREATE_SCENARIO_LAYER_LOSS_AGGREGATE_HEADER_TABLE = @"IF(OBJECT_ID('{0}') IS NULL)
CREATE TABLE {0}
(
  LossAggregateHeaderId INT IDENTITY(1, 1) NOT NULL
, LossAggregateName VARCHAR(100) NOT NULL
, ScenarioStartDate DATETIME2(7) NOT NULL
, ScenarioEndDate DATETIME2(7) NOT NULL
, CreationDate DATETIME2(7) NOT NULL
, CONSTRAINT pk_dbo_ScenarioLayerLossAggregateHeader PRIMARY KEY CLUSTERED(LossAggregateHeaderId ASC)
, CONSTRAINT uk_dbo_ScenarioLayerLossAggregateHeader_LossAggregateName_CreationDate UNIQUE(LossAggregateName, ScenarioStartDate, ScenarioEndDate, CreationDate)
);";
        private const string CREATE_SCENARIO_LAYER_LOSS_AGGREGATE_TABLE = @"IF(OBJECT_ID('{0}') IS NULL)
CREATE TABLE {0}
(
  LossAggregateHeaderId INT NOT NULL
, ScenarioId BIGINT NOT NULL
, EventDate DATETIME2(7) NOT NULL
, LayerId BIGINT NOT NULL
, LayerInceptionDate DATETIME2(7) NOT NULL
, LayerExpirationDate DATETIME2(7) NOT NULL
, SimulationInceptionDate DATETIME2(7) NOT NULL
, SimulationExpirationDate DATETIME2(7) NOT NULL
, SimulationUWYear INT NOT NULL
, IsFHCF BIT NOT NULL
, LAE DECIMAL(18, 10) NOT NULL
, LossCurrency VARCHAR(3) NOT NULL
, GULoss DECIMAL(18, 2) NOT NULL
, SectionsAdjustment DECIMAL(18, 2) NOT NULL
, OccLoss100Pct DECIMAL(18, 2) NOT NULL
, OccLoss DECIMAL(18, 2) NOT NULL
, LayerLoss100Pct DECIMAL(18, 2) NOT NULL
, LayerLoss DECIMAL(18, 2) NOT NULL
, AggLoss100Pct DECIMAL(18, 2) NOT NULL
, AggLoss DECIMAL(18, 2) NOT NULL
, LastCumulativeOccLoss100Pct DECIMAL(18, 2) NOT NULL
, LastAggLoss100Pct DECIMAL(18, 2) NOT NULL
, NewAggLimit DECIMAL(18, 2) NOT NULL
, NewAggRetention DECIMAL(18, 2) NOT NULL
, ReinstatementPremium100Pct DECIMAL(18, 2) NOT NULL
, ReinstatementPremium DECIMAL(18, 2) NOT NULL
, ReinstatementBrokerage100Pct DECIMAL(18, 2) NOT NULL
, ReinstatementBrokerage DECIMAL(18, 2) NOT NULL
, Placement DECIMAL(18, 10) NOT NULL
, CONSTRAINT pk_dbo_ScenarioLayerLossAggregate PRIMARY KEY CLUSTERED(LossAggregateHeaderId ASC, ScenarioId ASC, LayerId ASC, SimulationUWYear ASC)
, CONSTRAINT fk_dbo_ScenarioLayerLossAggregate_LossAggregateHeaderId_dbo_ScenarioLayerLossAggregateHeader_LossAggregateHeaderId FOREIGN KEY (LossAggregateHeaderId) REFERENCES dbo.ScenarioLayerLossAggregateHeader(LossAggregateHeaderId)
);";
        private const string ADD_SCENARIO_LAYER_LOSS_AGGREGATE_HEADER = @"
CREATE OR ALTER PROCEDURE {0}
  @LossAggregateName VARCHAR(200)
, @ScenarioStartDate DATETIME2(7)
, @ScenarioEndDate DATETIME2(7)
, @CreationDate DATETIME2(7)
AS
BEGIN
    SET NOCOUNT ON;
	INSERT INTO {1}
	(
	  LossAggregateName
	, ScenarioStartDate
	, ScenarioEndDate
	, CreationDate
	)
	VALUES
	(
	  @LossAggregateName
	, @ScenarioStartDate
	, @ScenarioEndDate
	, @CreationDate  
	);

	SELECT SCOPE_IDENTITY();
END;";

        #endregion Scenario Layer Loss Aggregate

        #region Layer Period Cessions

        private const string LAYER_PERIOD_CESSION_HEADER = "dbo.LayerPeriodCessionHeader";
        private const string LAYER_PERIOD_CESSION = "dbo.LayerPeriodCession";
        private const string ADD_LAYER_PERIOD_CESSION_HEADER_PROCEDURE = "dbo.AddLayerPeriodCessionHeader";

        private const string CREATE_LAYER_PERIOD_CESSION_HEADER_TABLE = @"IF(OBJECT_ID('{0}') IS NULL)
CREATE TABLE {0}
(
  LayerPeriodCessionHeaderId INT IDENTITY(1, 1) NOT NULL
, LayerPeriodCessionHeaderName VARCHAR(100) NOT NULL
, AsOfDate DATETIME2(7) NOT NULL
, CONSTRAINT pk_dbo_LayerPeriodCessionHeader PRIMARY KEY CLUSTERED(LayerPeriodCessionHeaderId ASC)
, CONSTRAINT uk_dbo_LayerPeriodCessionHeader_LayerPeriodCessionHeaderName_AsOfDate UNIQUE(LayerPeriodCessionHeaderName, AsOfDate)
);";

        private const string CREATE_LAYER_PERIOD_CESSION_TABLE = @"IF(OBJECT_ID('{0}') IS NULL)
CREATE TABLE {0}
(
  LayerPeriodCessionHeaderId INT NOT NULL
, RetroLevel TINYINT NOT NULL
, RetroProgramId BIGINT NOT NULL
, LayerId BIGINT NOT NULL
, StartInclusive DATETIME2(7) NOT NULL
, EndInclusive DATETIME2(7) NOT NULL
, NetCession DECIMAL(18, 10) NOT NULL
, CONSTRAINT pk_dbo_LayerPeriodCession PRIMARY KEY CLUSTERED(LayerPeriodCessionHeaderId ASC, RetroProgramId ASC, LayerId ASC, StartInclusive ASC, EndInclusive ASC)
, CONSTRAINT fk_dbo_LayerPeriodCession_LayerPeriodCessionHeaderId_dbo_LayerPeriodCessionHeader_LayerPeriodCessionHeaderId FOREIGN KEY (LayerPeriodCessionHeaderId) REFERENCES dbo.LayerPeriodCessionHeader(LayerPeriodCessionHeaderId)
);";

        private const string ADD_LAYER_PERIOD_CESSION_HEADER = @"
CREATE OR ALTER PROCEDURE {0}
  @LayerPeriodCessionHeaderName VARCHAR(200)
, @AsOfDate DATETIME2(7)
AS
BEGIN
    SET NOCOUNT ON;
	INSERT INTO {1}
	(
	  LayerPeriodCessionHeaderName
	, AsOfDate
	)
	VALUES
	(
	  @LayerPeriodCessionHeaderName
	, @AsOfDate  
	);

	SELECT SCOPE_IDENTITY();
END;";
        #endregion Layer Period Cessions

        #endregion Constants

        #region Variables

        private static object _scenarioLayerReinstatementCreationLock;
        private static object _layerLossAggregateHeaderCreationLock;
        private static object _layerLossAggregateCreationLock;
        private static object _layerPeriodCessionsHeaderCreationLock;
        private static object _layerPeriodCessionsCreationLock;

        #endregion Variables

        #region Methods

        partial void Initialise()
        {
            _scenarioLayerReinstatementCreationLock = new object();
            _layerLossAggregateHeaderCreationLock = new object();
            _layerLossAggregateCreationLock = new object();
            _layerPeriodCessionsHeaderCreationLock = new object();
            _layerPeriodCessionsCreationLock = new object();
        }

        #region Scenario Layer Reinstatement

        public void Save(IEnumerable<ScenarioLayerReinstatement> reinstatementEntries)
        {
            //using (TransactionScope transactionScope = new TransactionScope(TransactionScopeOption.Required, TransactionManager.MaximumTimeout))
            //{
                lock (_scenarioLayerReinstatementCreationLock)
                {
                    ExecuteSql(string.Format(CREATE_SCENARIO_LAYER_REINSTATEMENT_TABLE, SCENARIO_LAYER_REINSTATEMENT));
                }
                var reader = reinstatementEntries.ToObjectDataReader<ScenarioLayerReinstatement>();
                WriteToTable(reader, SCENARIO_LAYER_REINSTATEMENT);
            //    transactionScope.Complete();
            //}
        }

        #endregion Scenario Layer Reinstatement

        #region Scenario Layer Loss Aggregate

        private void AddScenarioLayerLossAggregateHeader(ScenarioLayerLossAggregateHeader header)
        {
            lock (_layerLossAggregateHeaderCreationLock)
            {
                ExecuteSql(string.Format(CREATE_SCENARIO_LAYER_LOSS_AGGREGATE_HEADER_TABLE, SCENARIO_LAYER_LOSS_AGGREGATE_HEADER));
                ExecuteSql(string.Format(ADD_SCENARIO_LAYER_LOSS_AGGREGATE_HEADER, ADD_SCENARIO_LAYER_LOSS_AGGREGATE_HEADER_PROCEDURE, SCENARIO_LAYER_LOSS_AGGREGATE_HEADER));
            }

            header.LossAggregateHeaderId = (int)(decimal)ExecuteScalar(ADD_SCENARIO_LAYER_LOSS_AGGREGATE_HEADER_PROCEDURE, header.LossAggregateHeaderName, header.ScenarioStartDate, header.ScenarioEndDate, header.CreationDate);
        }

        public void Save(ScenarioLayerLossAggregateHeader header, IEnumerable<DtoLayeringOutput> exposureEntries)
        {
            //using (TransactionScope transactionScope = new TransactionScope(TransactionScopeOption.Required, TransactionManager.MaximumTimeout))
            //{
                AddScenarioLayerLossAggregateHeader(header);
                lock (_layerLossAggregateCreationLock)
                {
                    ExecuteSql(string.Format(CREATE_SCENARIO_LAYER_LOSS_AGGREGATE_TABLE, SCENARIO_LAYER_LOSS_AGGREGATE));
                }
                var reader = exposureEntries.Select(x => new ScenarioLayerLossAggregate(x) { LossAggregateHeaderId = header.LossAggregateHeaderId }).ToObjectDataReader<ScenarioLayerLossAggregate>();
                WriteToTable(reader, SCENARIO_LAYER_LOSS_AGGREGATE); 
            //    transactionScope.Complete();
            //}
        }

        #endregion Scenario Layer Loss Aggregate

        #region Layer Period Cession

        private void AddLayerPeriodCessionHeader(LayerPeriodCessionHeader header)
        {
            lock (_layerPeriodCessionsHeaderCreationLock)
            {
                ExecuteSql(string.Format(CREATE_LAYER_PERIOD_CESSION_HEADER_TABLE, LAYER_PERIOD_CESSION_HEADER));
                ExecuteSql(string.Format(ADD_LAYER_PERIOD_CESSION_HEADER, ADD_LAYER_PERIOD_CESSION_HEADER_PROCEDURE, LAYER_PERIOD_CESSION_HEADER));
            }

            header.LayerPeriodCessionHeaderId = (int)(decimal)ExecuteScalar(ADD_LAYER_PERIOD_CESSION_HEADER_PROCEDURE, header.LayerPeriodCessionHeaderName, header.AsOfDate);
        }

        public void Save(LayerPeriodCessionHeader header, IEnumerable<LayerPeriodCession> layerPeriodCessions)
        {
            //using (TransactionScope transactionScope = new TransactionScope(TransactionScopeOption.Required, TransactionManager.MaximumTimeout))
            //{
                AddLayerPeriodCessionHeader(header);
                lock (_layerLossAggregateCreationLock)
                {
                    ExecuteSql(string.Format(CREATE_LAYER_PERIOD_CESSION_TABLE, LAYER_PERIOD_CESSION));
                }
                var reader = layerPeriodCessions.Select(x => new LayerPeriodCessionOutput(ref x) { LayerPeriodCessionHeaderId = header.LayerPeriodCessionHeaderId }).ToObjectDataReader<LayerPeriodCessionOutput>();
                WriteToTable(reader, LAYER_PERIOD_CESSION);
            //    transactionScope.Complete();
            //}
        }

        #endregion Layer Period Cession

        #endregion Methods
    }
}
