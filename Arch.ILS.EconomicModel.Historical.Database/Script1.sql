
--/*Drop Stored Procedures*/

--IF(OBJECT_ID('dbo.AddLayerPeriodCessionHeader', 'P') IS NOT NULL)
--	DROP PROCEDURE dbo.AddLayerPeriodCessionHeader;
--GO

--IF(OBJECT_ID('dbo.AddScenarioLayerLossAggregateHeader', 'P') IS NOT NULL)
--	DROP PROCEDURE dbo.AddScenarioLayerLossAggregateHeader;
--GO

--/*Drop Tables*/
--IF(OBJECT_ID('dbo.LayerPeriodCession') IS NOT NULL)
--	DROP TABLE dbo.LayerPeriodCession;
--GO

--IF(OBJECT_ID('dbo.LayerPeriodCessionHeader') IS NOT NULL)
--	DROP TABLE dbo.LayerPeriodCessionHeader;
--GO

--IF(OBJECT_ID('dbo.ScenarioLayerLossAggregate') IS NOT NULL)
--	DROP TABLE dbo.ScenarioLayerLossAggregate;
--GO

--IF(OBJECT_ID('dbo.ScenarioLayerLossAggregateHeader') IS NOT NULL)
--	DROP TABLE dbo.ScenarioLayerLossAggregateHeader;
--GO


--/*Create Tables*/
--CREATE TABLE dbo.ScenarioLayerLossAggregateHeader
--(
--  LossAggregateHeaderId INT IDENTITY(1, 1) NOT NULL
--, LossAggregateName VARCHAR(200) NOT NULL
--, ScenarioStartDate DATETIME2(7) NOT NULL
--, ScenarioEndDate DATETIME2(7) NOT NULL
--, CreationDate DATETIME2(7) NOT NULL
--, CONSTRAINT pk_dbo_ScenarioLayerLossAggregateHeader PRIMARY KEY CLUSTERED(LossAggregateHeaderId ASC)
--, CONSTRAINT uk_dbo_ScenarioLayerLossAggregateHeader_LossAggregateName_CreationDate UNIQUE(LossAggregateName, ScenarioStartDate, ScenarioEndDate, CreationDate)
--);
--GO

--CREATE TABLE dbo.ScenarioLayerLossAggregate
--(
--  LossAggregateHeaderId INT NOT NULL
--, ScenarioId BIGINT NOT NULL
--, EventDate DATETIME2(7) NOT NULL
--, LayerId BIGINT NOT NULL
--, LayerInceptionDate DATETIME2(7) NOT NULL
--, LayerExpirationDate DATETIME2(7) NOT NULL
--, SimulationInceptionDate DATETIME2(7) NOT NULL
--, SimulationExpirationDate DATETIME2(7) NOT NULL
--, SimulationUWYear INT NOT NULL
--, IsFHCF BIT NOT NULL
--, LAE DECIMAL(18, 10) NOT NULL
--, LossCurrency VARCHAR(3) NOT NULL
--, GULoss DECIMAL(18, 2) NOT NULL
--, LayerLoss DECIMAL(18, 2) NOT NULL
--, SectionsAdjustment DECIMAL(18, 2) NOT NULL
--, LastCumulativeOccLoss DECIMAL(18, 2) NOT NULL
--, LastAggLoss DECIMAL(18, 2) NOT NULL
--, OccLoss DECIMAL(18, 2) NOT NULL
--, AggLoss DECIMAL(18, 2) NOT NULL
--, NewAggLimit DECIMAL(18, 2) NOT NULL
--, NewAggRetention DECIMAL(18, 2) NOT NULL
--, CONSTRAINT pk_dbo_ScenarioLayerLossAggregate PRIMARY KEY CLUSTERED(LossAggregateHeaderId ASC, ScenarioId ASC, LayerId ASC, SimulationUWYear ASC)
--, CONSTRAINT fk_dbo_ScenarioLayerLossAggregate_LossAggregateHeaderId_dbo_ScenarioLayerLossAggregateHeader_LossAggregateHeaderId FOREIGN KEY (LossAggregateHeaderId) REFERENCES dbo.ScenarioLayerLossAggregateHeader(LossAggregateHeaderId)
--);
--GO

--CREATE TABLE dbo.LayerPeriodCessionHeader
--(
--  LayerPeriodCessionHeaderId INT IDENTITY(1, 1) NOT NULL
--, LayerPeriodCessionHeaderName VARCHAR(100) NOT NULL
--, AsOfDate DATETIME2(7) NOT NULL
--, CONSTRAINT pk_dbo_LayerPeriodCessionHeader PRIMARY KEY CLUSTERED(LayerPeriodCessionHeaderId ASC)
--, CONSTRAINT uk_dbo_LayerPeriodCessionHeader_LayerPeriodCessionHeaderName_AsOfDate UNIQUE(LayerPeriodCessionHeaderName, AsOfDate)
--);
--GO

--CREATE TABLE dbo.LayerPeriodCession
--(
--  LayerPeriodCessionHeaderId INT NOT NULL
--, RetroLevel TINYINT NOT NULL
--, RetroProgramId BIGINT NOT NULL
--, LayerId BIGINT NOT NULL
--, StartInclusive DATETIME2(7) NOT NULL
--, EndInclusive DATETIME2(7) NOT NULL
--, NetCession DECIMAL(18, 10) NOT NULL
--, CONSTRAINT pk_dbo_LayerPeriodCession PRIMARY KEY CLUSTERED(LayerPeriodCessionHeaderId ASC, RetroProgramId ASC, LayerId ASC, StartInclusive ASC, EndInclusive ASC)
--, CONSTRAINT fk_dbo_LayerPeriodCession_LayerPeriodCessionHeaderId_dbo_LayerPeriodCessionHeader_LayerPeriodCessionHeaderId FOREIGN KEY (LayerPeriodCessionHeaderId) REFERENCES dbo.LayerPeriodCessionHeader(LayerPeriodCessionHeaderId)
--);
--GO

--/*Create Stored Procedures*/
--CREATE PROCEDURE dbo.AddScenarioLayerLossAggregateHeader
--  @LossAggregateName VARCHAR(100)
--, @ScenarioStartDate DATETIME2(7)
--, @ScenarioEndDate DATETIME2(7)
--, @CreationDate DATETIME2(7)
--AS
--BEGIN
--	SET NOCOUNT ON;
--	INSERT INTO dbo.ScenarioLayerLossAggregateHeader
--	(
--	  LossAggregateName
--	, ScenarioStartDate
--	, ScenarioEndDate
--	, CreationDate
--	)
--	VALUES
--	(
--	  @LossAggregateName
--	, @ScenarioStartDate
--	, @ScenarioEndDate
--	, @CreationDate  
--	);

--	SELECT SCOPE_IDENTITY();
--END;
--GO

--CREATE PROCEDURE dbo.AddLayerPeriodCessionHeader
--  @LayerPeriodCessionHeaderName VARCHAR(200)
--, @AsOfDate DATETIME2(7)
--AS
--BEGIN
--    SET NOCOUNT ON;
--	INSERT INTO dbo.LayerPeriodCessionHeader
--	(
--	  LayerPeriodCessionHeaderName
--	, AsOfDate
--	)
--	VALUES
--	(
--	  @LayerPeriodCessionHeaderName
--	, @AsOfDate  
--	);

--	SELECT SCOPE_IDENTITY();
--END;