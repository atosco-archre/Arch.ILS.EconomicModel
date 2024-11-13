
using System.Data;
using System.Data.SqlClient;
using Org.BouncyCastle.Tls.Crypto.Impl.BC;
using Studio.Core.Sql;

namespace Arch.ILS.EconomicModel.Historical
{
    public partial class ScenarioSqlRepository : SqlRepository
    {
        #region Constants

        private const string GET_SCENARIO = @"SELECT ScenarioId
      ,Name
      ,Description
      ,Notes
      ,AnalysisStatus
      ,ScenarioStatus
      ,UseAdjustedMarketShare
      ,UseTrendedLosses
      ,Currency
      ,InforceDate
      ,FXRatesDate
      ,IsActive
      ,IsDeleted
      ,CreatedDate
      ,CreatedBy
      ,ModifiedDate
      ,ModifiedBy
      ,StepsJson
      ,IsOfficial
      ,ScenarioType
      ,RunDate
  FROM dbo.Scenario";

        private const string GET_SCENARIOLOSSEVENT = @"SELECT ScenarioId
      ,EventKey
      ,Name
      ,AutoName
      ,EventDate
      ,LossCurrency
      ,TrendedLoss
      ,UntrendedLoss
      ,EventYear
      ,PerilCode
      ,PerilName
      ,IsActive
      ,IsDeleted
      ,CreatedDate
      ,CreatedBy
      ,ModifiedDate
      ,ModifiedBy
      ,DtoLossEventJson
  FROM dbo.ScenarioLossEvent";

        private const string GET_SCENARIOLAYER = @"SELECT  ScenarioId
      ,CONVERT(INT, LayerId) AS LayerId
      --,RegisMKey
      --,CONVERT(INT, SubmissionId) AS SubmissionId
      --,LayerNum
      --,SubLayerNum
      --,ReinstCount
      ,Placement
      ,OccLimit
      ,OccRetention
      --,CascadeRetention
      --,AAD
      ,AggLimit
      ,AggRetention
      ,Franchise
      ,FranchiseReverse
      ,RiskLimit
      ,Inception
      --,UWYear
      ,Expiration
      --,ExpirationFinal
      --,Facility
      --,Segment
      --,LOB
      --,ContractType
      ,LimitBasis
      --,AttachBasis
      --,LAETerm
      --,ROL
      --,QuoteROL
      --,EstimatedShare
      --,SignedShare
      --,AuthShare
      --,QuotedShare
      --,Status
      --,LayerDesc
      --,Notes
      --,Rate
      --,PremiumFreq
      --,LayerType
      --,CreateDate
      ,IsActive
      ,IsDeleted
      --,InuringLimit
      ,RiskRetention
      --,LayerCategory
      --,LayerCatalog
      ,Premium
      --,QuotePremium
      --,RelShare
      --,TargetNetShare
      --,BoundFXRate
      --,BoundFXDate
      --,ERCActual
      --,ERCActualSource
      --,ELMarketShare
      --,ELHistoricalBurn
      --,ELBroker
      --,CONVERT(INT--, ProgramId) AS ProgramId
      --,ProgramExtName
      --,CedentId
      --,CedentName
      --,CedentGroupId
      --,CedentGroupName
      --,DepartmentId
      --,DepartmentName
      --,OfficeId
      --,OfficeName
      --,CONVERT(INT--, CompanyId) AS CompanyId
      --,CompanyName
      --,UnderwriterId
      --,Underwriter
      --,UnderwriterName
      --,SubmissionTranType
      --,Currency
      --,LegalEntity
      --,ModifiedDate
      --,ModifiedBy
      --,ReinstPercent
      --,CompanyCode
      --,RiskRegion
      --,RiskZone
  FROM dbo.ScenarioLayer";

        private const string GET_SCENARIOLAYERBYPARTITION = GET_SCENARIOLAYER + " WHERE (ScenarioId % {1}) = {0} ";

        private const string GET_SCENARIOLAYERBYSCENARIOID = GET_SCENARIOLAYER + " WHERE ScenarioId = {0}";

        private const string GET_SCENARIOLAYERBYLAYERID = GET_SCENARIOLAYER + " WHERE LayerId = {0}";

        private const string GET_SCENARIOLAYERLOSS = @"SELECT ScenarioId
      --,EventKey
      ,CONVERT(INT, LayerId) AS LayerId
      --,CONVERT(INT, SubmissionId) AS SubmissionId
      --,CONVERT(INT, ProgramId) AS ProgramId
      --,CONVERT(INT, CedentId) AS CedentId
      ,IsFHCF
      ,LAE
      ,LossCurrency
      ,FXRateToLayerCurrency
      ,LayerCurrency
      ,GULoss
      --,GULossWithLAE
      --,LayerLoss
      --,GrossLoss
      --,NetLoss
      --,OccGrossLoss
      --,OccNetLoss
      --,SectionsAdjustment
      --,LastCumulativeOccLoss
      --,LastAggLoss
      --,OccLoss
      --,AggLoss
      --,NewAggLimit
      --,NewAggRetention
      --,NewAggLimitInLayerCurrency
      --,NewAggRetentionInLayerCurrency
      --,CalculationStatus
      --,GrossTranType
      --,NetTranType
      --,TotalNetRetroCession
      --,GULossBasedOnAdjustedMKS
      --,GULossBasedOnMKS
      --,GULossOverridden
      --,CededReinstPremium
      --,GrossReinstPremium
      --,LimitUsed
      --,LossCeded
      --,NetReinstPremium
      --,OccLossCeded
      --,ReinstPremium
      --,MarketShare
      --,CompanyCode
      --,RiskRegion
      --,RiskZone
  FROM dbo.ScenarioLayerLoss";

        private const string GET_SCENARIOLAYERLOSSBYPARTITION = GET_SCENARIOLAYERLOSS + " WHERE (ScenarioId % {1}) = {0} ";

        private const string GET_SCENARIOLAYERLOSSBYSCENARIOID = GET_SCENARIOLAYERLOSS + " WHERE ScenarioId = {0}";

        private const string GET_SCENARIOLAYERLOSSBYLAYERID = GET_SCENARIOLAYERLOSS + " WHERE LayerId = {0}";

        private const string GET_SCENARIOOCCLAYERLOSS = GET_SCENARIOLAYERLOSS + " WHERE (GULoss != 0 OR GrossLoss != 0 OR LayerLoss != 0)";

        private const string GET_SCENARIOLAYERSECTION = @"SELECT ScenarioId
      ,LayerId
      ,SectionId
      ,RollUpType
      ,FXRateToParent
  FROM dbo.ScenarioLayerSection";

        private const string GET_SCENARIOLAYERSECTIONBYPARTITION = GET_SCENARIOLAYERSECTION + " WHERE (ScenarioId % {1}) = {0} ";

        private const string GET_SCENARIOLAYERREINSTATEMENT = @"SELECT ScenarioId
      ,LayerId
      ,ReinstatementId
      ,ReinstatementOrder
      ,Quantity
      ,PremiumShare
      ,BrokeragePercentage
      ,RowVersion AS RowVersion
  FROM dbo.ScenarioLayerReinstatement";

        private const string GET_SCENARIORETROCESSION = @"SELECT ScenarioId
      ,RetroAllocationId
      ,LayerId
      ,RetroInvestorId
      ,IsActive
      ,IsDeleted
      ,CessionNet
      ,CessionGross
      ,CessionCapFactor
      ,CessionCapFactorSent
      ,CessionGrossFinalSent
      ,CessionNetFinalSent
      ,AllocationStatus
      ,Override
      ,RetroProgramId
      ,RetroProgramName
      ,RetroLevelType
      ,RetroProfileId
      ,RetroProfileName
      ,RetroInvestorName
      ,Expiration
      ,Inception
  FROM dbo.ScenarioRetroCession";

        private const string GET_SCENARIORETROCESSIONLOSS = @"SELECT ScenarioId
      ,EventKey
      ,RetroAllocationId
      ,LayerId
      ,RetroInvestorId
      ,Currency
      ,Loss
      ,OccLoss
      ,ReinstPremium
  FROM dbo.ScenarioRetroCessionLoss";

        #endregion Constants

        #region Constructor

        public ScenarioSqlRepository(string connectionString, int timeOut = DefaultTimeout) : base(connectionString, timeOut)
        {
            Initialise();
        }

        #endregion Constructor

        #region Methods

        partial void Initialise();

        public Task<IList<Scenario>> GetScenarios()
        {
            return Task.Factory.StartNew<IList<Scenario>>(() =>
            {
                using (var reader = (SqlDataReader)ExecuteReaderSql(GET_SCENARIO))
                {
                    return ReadScenarios(reader);
                }
            });
        }

        private IList<Scenario> ReadScenarios(IDataReader reader)
        {
            IList<Scenario> scenarios = new List<Scenario>();
            while (reader.Read())
            {
                int index = 0;
                Scenario scenario = new Scenario
                {
                    ScenarioId = reader.GetInt64(index),
                    Name = reader.GetString(++index),
                    Description = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    Notes = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    AnalysisStatus = reader.GetInt32(++index),
                    ScenarioStatus = reader.GetInt32(++index),
                    UseAdjustedMarketShare = reader.GetBoolean(++index),
                    UseTrendedLosses = reader.GetBoolean(++index),
                    Currency = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    InforceDate = reader.IsDBNull(++index) ? null : reader.GetDateTime(index).Date,
                    FXRatesDate = reader.IsDBNull(++index) ? null : reader.GetDateTime(index).Date,
                    IsActive = reader.GetBoolean(++index),
                    IsDeleted = reader.GetBoolean(++index),
                    CreatedDate = reader.GetDateTime(++index),
                    CreatedBy = reader.GetString(++index),
                    ModifiedDate = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                    ModifiedBy = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    StepsJson = reader.GetString(++index),
                    IsOfficial = reader.GetBoolean(++index),
                    ScenarioType = reader.IsDBNull(++index) ? null : reader.GetInt64(index),
                    RunDate = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                };
                scenarios.Add(scenario);
            }
            return scenarios;
        }

        public Task<IList<ScenarioLossEvent>> GetScenarioLossEvents()
        {
            return Task.Factory.StartNew(() =>
            {
                using (var reader = (SqlDataReader)ExecuteReaderSql(GET_SCENARIOLOSSEVENT))
                {
                    return ReadScenarioLossEvents(reader);
                }
            });
        }

        private IList<ScenarioLossEvent> ReadScenarioLossEvents(IDataReader reader)
        {
            IList<ScenarioLossEvent> scenarioLossEvents = new List<ScenarioLossEvent>();
            while (reader.Read())
            {
                int index = 0;
                ScenarioLossEvent scenarioLossEvent = new ScenarioLossEvent
                {
                    ScenarioId = reader.GetInt64(index),
                    EventKey = reader.GetString(++index),
                    Name = reader.GetString(++index),
                    AutoName = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    EventDate = reader.GetDateTime(++index).Date,
                    LossCurrency = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    TrendedLoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    UntrendedLoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    EventYear = reader.GetInt32(++index),
                    PerilCode = reader.GetString(++index),
                    PerilName = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    IsActive = reader.GetBoolean(++index),
                    IsDeleted = reader.GetBoolean(++index),
                    CreatedDate = reader.GetDateTime(++index),
                    CreatedBy = reader.GetString(++index),
                    ModifiedDate = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                    ModifiedBy = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    DtoLossEventJson = reader.GetString(++index),
                };
                scenarioLossEvents.Add(scenarioLossEvent);
            }
            return scenarioLossEvents;
        }

        public Task<IEnumerable<ScenarioLayer>> GetScenarioLayers(int partitionCount = 8)
        {
            return Task.Factory.StartNew(() =>
            {
                ScenarioLayer[][] scenarioLayers = new ScenarioLayer[partitionCount][];
                Task[] scenarioLayersTasks = new Task[partitionCount];
                for (int i = 0; i < scenarioLayersTasks.Length; i++)
                    scenarioLayersTasks[i] = Task.Factory.StartNew(state =>
                    {
                        int index = (int)state!;
                        using (var reader = (SqlDataReader)ExecuteReaderSql(string.Format(GET_SCENARIOLAYERBYPARTITION, index, partitionCount)))
                        {
                            scenarioLayers[index] = ReadScenarioLayers(reader).ToArray();
                        }
                    }, i);
                Task.WaitAll(scenarioLayersTasks);
                return scenarioLayers.SelectMany(x => x);
            });
        }

        public Task<IList<ScenarioLayer>> GetScenarioLayers(long scenarioId)
        {
            return Task.Factory.StartNew<IList<ScenarioLayer>>(() =>
            {
                using (var reader = (SqlDataReader)ExecuteReaderSql(string.Format(GET_SCENARIOLAYERBYSCENARIOID, scenarioId)))
                {
                    return ReadScenarioLayers(reader).ToList();
                }
            });
        }

        public Task<IList<ScenarioLayer>> GetScenarioLayersByLayerId(long layerId)
        {
            return Task.Factory.StartNew<IList<ScenarioLayer>>(() =>
            {
                using (var reader = (SqlDataReader)ExecuteReaderSql(string.Format(GET_SCENARIOLAYERBYLAYERID, layerId)))
                {
                    return ReadScenarioLayers(reader).ToList();
                }
            });
        }

        private IEnumerable<ScenarioLayer> ReadScenarioLayers(IDataReader reader)
        {
            while (reader.Read())
            {
                int index = 0;
                yield return new ScenarioLayer
                {
                    ScenarioId = reader.GetInt64(index),
                    LayerId = reader.GetInt32(++index),
                    //RegisMKey = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    //SubmissionId = reader.GetInt32(++index),
                    //LayerNum = reader.GetInt32(++index),
                    //SubLayerNum = reader.GetInt32(++index),
                    //ReinstCount = reader.GetInt32(++index),
                    Placement = reader.GetDecimal(++index),
                    OccLimit = reader.GetDecimal(++index),
                    OccRetention = reader.GetDecimal(++index),
                    //CascadeRetention = reader.GetDecimal(++index),
                    //AAD = reader.GetDecimal(++index),
                    AggLimit = reader.GetDecimal(++index),
                    AggRetention = reader.GetDecimal(++index),
                    Franchise = reader.GetDecimal(++index),
                    FranchiseReverse = reader.GetDecimal(++index),
                    RiskLimit = reader.GetDecimal(++index),
                    Inception = reader.GetDateTime(++index).Date,
                    //UWYear = reader.GetInt32(++index),
                    Expiration = reader.GetDateTime(++index).Date,
                    //ExpirationFinal = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                    //Facility = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    //Segment = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    //LOB = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    //ContractType = reader.GetInt32(++index),
                    LimitBasis = reader.GetInt32(++index),
                    //AttachBasis = reader.GetInt32(++index),
                    //LAETerm = reader.GetInt32(++index),
                    //ROL = reader.GetDecimal(++index),
                    //QuoteROL = reader.GetDecimal(++index),
                    //EstimatedShare = reader.GetDecimal(++index),
                    //SignedShare = reader.GetDecimal(++index),
                    //AuthShare = reader.GetDecimal(++index),
                    //QuotedShare = reader.GetDecimal(++index),
                    //Status = reader.GetInt32(++index),
                    //LayerDesc = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    //Notes = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    //Rate = reader.GetDecimal(++index),
                    //PremiumFreq = reader.GetInt32(++index),
                    //LayerType = reader.GetInt32(++index),
                    //CreateDate = reader.GetDateTime(++index),
                    IsActive = reader.GetBoolean(++index),
                    IsDeleted = reader.GetBoolean(++index),
                    //InuringLimit = reader.GetDecimal(++index),
                    RiskRetention = reader.GetDecimal(++index),
                    //LayerCategory = reader.GetInt32(++index),
                    //LayerCatalog = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    //Premium = reader.GetDecimal(++index),
                    //QuotePremium = reader.GetDecimal(++index),
                    //RelShare = reader.GetDecimal(++index),
                    //TargetNetShare = reader.GetDecimal(++index),
                    //BoundFXRate = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //BoundFXDate = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                    //ERCActual = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //ERCActualSource = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    //ELMarketShare = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //ELHistoricalBurn = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //ELBroker = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //ProgramId = reader.GetInt32(++index),
                    //ProgramExtName = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    //CedentId = reader.GetInt64(++index),
                    //CedentName = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    //CedentGroupId = reader.GetInt64(++index),
                    //CedentGroupName = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    //DepartmentId = reader.GetInt64(++index),
                    //DepartmentName = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    //OfficeId = reader.GetInt64(++index),
                    //OfficeName = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    //CompanyId = reader.GetInt32(++index),
                    //CompanyName = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    //UnderwriterId = reader.GetInt64(++index),
                    //Underwriter = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    //UnderwriterName = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    //SubmissionTranType = reader.GetInt32(++index),
                    //Currency = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    //LegalEntity = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    //ModifiedDate = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                    //ModifiedBy = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    //ReinstPercent = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //CompanyCode = reader.IsDBNull(++index) ? null : reader.GetInt64(index),
                    //RiskRegion = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    //RiskZone = reader.IsDBNull(++index) ? null : reader.GetString(index),
                };
            }
        }

        public Task<IEnumerable<ScenarioLayerLoss>> GetScenarioLayerLosses(int partitionCount = 8)
        {
            return Task.Factory.StartNew(() =>
            {
                ScenarioLayerLoss[][] scenarioLayerLosses = new ScenarioLayerLoss[partitionCount][];
                Task[] scenarioLayerLossesTasks = new Task[partitionCount];
                for (int i = 0; i < scenarioLayerLossesTasks.Length; i++)
                    scenarioLayerLossesTasks[i] = Task.Factory.StartNew(state =>
                    {
                        int index = (int)state!;
                        using (var reader = (SqlDataReader)ExecuteReaderSql(string.Format(GET_SCENARIOLAYERLOSSBYPARTITION, index, partitionCount)))
                        {
                            scenarioLayerLosses[index] = ReadScenarioLayerLoss(reader).ToArray();
                        }
                    }, i);
                Task.WaitAll(scenarioLayerLossesTasks);
                return scenarioLayerLosses.SelectMany(x => x);
            });
        }

        public Task<IList<ScenarioLayerLoss>> GetScenarioLayerLosses(long scenarioId)
        {
            return Task.Factory.StartNew<IList<ScenarioLayerLoss>>(() =>
            {
                using (var reader = (SqlDataReader)ExecuteReaderSql(string.Format(GET_SCENARIOLAYERLOSSBYSCENARIOID, scenarioId)))
                {
                    return ReadScenarioLayerLoss(reader).ToList();
                }
            });
        }

        public Task<IList<ScenarioLayerLoss>> GetScenarioLayerLossesByLayerId(long layerId)
        {
            return Task.Factory.StartNew<IList<ScenarioLayerLoss>>(() =>
            {
                using (var reader = (SqlDataReader)ExecuteReaderSql(string.Format(GET_SCENARIOLAYERLOSSBYLAYERID, layerId)))
                {
                    return ReadScenarioLayerLoss(reader).ToList();
                }
            });
        }

        public Task<IList<ScenarioLayerLoss>> GetScenarioOccLayerLosses()
        {
            return Task.Factory.StartNew<IList<ScenarioLayerLoss>>(() =>
            {
                using (var reader = (SqlDataReader)ExecuteReaderSql(GET_SCENARIOOCCLAYERLOSS))
                {
                    return ReadScenarioLayerLoss(reader).ToList();
                }
            });
        }

        private IEnumerable<ScenarioLayerLoss> ReadScenarioLayerLoss(IDataReader reader)
        {
            while (reader.Read())
            {
                int index = 0;
                yield return new ScenarioLayerLoss
                {
                    ScenarioId = reader.GetInt64(index),
                    //EventKey = reader.GetString(++index),
                    LayerId = reader.GetInt32(++index),
                    //SubmissionId = reader.GetInt32(++index),
                    //ProgramId = reader.GetInt32(++index),
                    //CedentId = reader.GetInt64(++index),
                    IsFHCF = reader.GetBoolean(++index),
                    LAE = reader.GetDecimal(++index),
                    LossCurrency = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    FXRateToLayerCurrency = reader.GetDecimal(++index),
                    LayerCurrency = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    GULoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //GULossWithLAE = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //LayerLoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //GrossLoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //NetLoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //OccGrossLoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //OccNetLoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //SectionsAdjustment = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //LastCumulativeOccLoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //LastAggLoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //OccLoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //AggLoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //NewAggLimit = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //NewAggRetention = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //NewAggLimitInLayerCurrency = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //NewAggRetentionInLayerCurrency = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //CalculationStatus = reader.GetInt32(++index),
                    //GrossTranType = reader.GetInt32(++index),
                    //NetTranType = reader.GetInt32(++index),
                    //TotalNetRetroCession = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //GULossBasedOnAdjustedMKS = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //GULossBasedOnMKS = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //GULossOverridden = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //CededReinstPremium = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //GrossReinstPremium = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //LimitUsed = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //LossCeded = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //NetReinstPremium = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //OccLossCeded = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //ReinstPremium = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //MarketShare = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    //CompanyCode = reader.IsDBNull(++index) ? null : reader.GetInt64(index),
                    //RiskRegion = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    //RiskZone = reader.IsDBNull(++index) ? null : reader.GetString(index)
                };
            }
        }

        public Task<IEnumerable<ScenarioLayerSection>> GetScenarioLayerSections(int partitionCount = 8)
        {
            return Task.Factory.StartNew(() =>
            {
                ScenarioLayerSection[][] scenarioLayerSections = new ScenarioLayerSection[partitionCount][];
                Task[] scenarioLayerSectionsTasks = new Task[partitionCount];
                for (int i = 0; i < scenarioLayerSectionsTasks.Length; i++)
                    scenarioLayerSectionsTasks[i] = Task.Factory.StartNew(state =>
                    {
                        int index = (int)state!;
                        using (var reader = (SqlDataReader)ExecuteReaderSql(string.Format(GET_SCENARIOLAYERSECTIONBYPARTITION, index, partitionCount)))
                        {
                            scenarioLayerSections[index] = ReadScenarioLayerSections(reader).ToArray();
                        }
                    }, i);
                Task.WaitAll(scenarioLayerSectionsTasks);
                return scenarioLayerSections.SelectMany(x => x);
            });
        }

        //public Task<IList<ScenarioLayerSection>> GetScenarioLayerSections()
        //{
        //    return Task.Factory.StartNew<IList<ScenarioLayerSection>>(() =>
        //    {
        //        using (var reader = (SqlDataReader)ExecuteReaderSql(GET_SCENARIOLAYERSECTION))
        //        {
        //            return ReadScenarioLayerSections(reader).ToList();
        //        }
        //    });
        //}

        private IEnumerable<ScenarioLayerReinstatement> ReadScenarioLayerReinstatements(IDataReader reader)
        {
            while (reader.Read())
            {
                int index = 0;
                byte[] rowVersionBuffer = new byte[8];
                long rowVersionLength = reader.GetBytes(7, 0, rowVersionBuffer, 0, 8);
                yield return new ScenarioLayerReinstatement
                (
                    ScenarioId: reader.GetInt64(index),
                    LayerId: reader.GetInt64(++index),
                    ReinstatementId: reader.GetInt32(++index),
                    ReinstatementOrder: reader.GetInt32(++index),
                    Quantity: reader.GetDouble(++index),
                    PremiumShare: reader.GetDecimal(++index),
                    BrokeragePercentage: reader.GetDecimal(++index),
                    RowVersion: rowVersionBuffer
                );
            }
        }

        public Task<IList<ScenarioLayerReinstatement>> GetScenarioLayerReinstatements()
        {
            return Task.Factory.StartNew<IList<ScenarioLayerReinstatement>>(() =>
            {
                using (var reader = (SqlDataReader)ExecuteReaderSql(GET_SCENARIOLAYERREINSTATEMENT))
                {
                    return ReadScenarioLayerReinstatements(reader).ToList();
                }
            });
        }

        private IEnumerable<ScenarioLayerSection> ReadScenarioLayerSections(IDataReader reader)
        {
            while (reader.Read())
            {
                int index = 0;
                yield return new ScenarioLayerSection
                {
                    ScenarioId = reader.GetInt64(index),
                    LayerId = reader.GetInt64(++index),
                    SectionId = reader.GetInt64(++index),
                    RollUpType = reader.GetInt32(++index),
                    FXRateToParent = reader.GetDecimal(++index),
                };
            }
        }

        public Task<IList<ScenarioRetroCession>> GetScenarioRetroCessions()
        {
            return Task.Factory.StartNew<IList<ScenarioRetroCession>>(() =>
            {
                using (var reader = (SqlDataReader)ExecuteReaderSql(GET_SCENARIORETROCESSION))
                {
                    return ReadScenarioRetroCessions(reader).ToList();
                }
            });
        }

        private IEnumerable<ScenarioRetroCession> ReadScenarioRetroCessions(IDataReader reader)
        {
            while (reader.Read())
            {
                int index = 0;
                yield return new ScenarioRetroCession
                {
                    ScenarioId = reader.GetInt64(index),
                    RetroAllocationId = reader.GetInt64(++index),
                    LayerId = reader.GetInt64(++index),
                    RetroInvestorId = reader.GetInt64(++index),
                    IsActive = reader.GetBoolean(++index),
                    IsDeleted = reader.GetBoolean(++index),
                    CessionNet = reader.GetDecimal(++index),
                    CessionGross = reader.GetDecimal(++index),
                    CessionCapFactor = reader.GetDecimal(++index),
                    CessionCapFactorSent = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    CessionGrossFinalSent = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    CessionNetFinalSent = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    AllocationStatus = reader.GetInt32(++index),
                    Override = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    RetroProgramId = reader.GetInt64(++index),
                    RetroProgramName = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    RetroLevelType = reader.GetInt32(++index),
                    RetroProfileId = reader.GetInt64(++index),
                    RetroProfileName = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    RetroInvestorName = reader.IsDBNull(++index) ? null : reader.GetString(index),
                    Expiration = reader.IsDBNull(++index) ? null : reader.GetDateTime(index).Date,
                    Inception = reader.IsDBNull(++index) ? null : reader.GetDateTime(index).Date
                };
            }
        }

        public Task<IList<ScenarioRetroCessionLoss>> GetScenarioRetroCessionLosses()
        {
            return Task.Factory.StartNew<IList<ScenarioRetroCessionLoss>>(() =>
            {
                using (var reader = (SqlDataReader)ExecuteReaderSql(GET_SCENARIORETROCESSIONLOSS))
                {
                    return ReadScenarioRetroCessionLossess(reader).ToList();
                }
            });
        }

        private IEnumerable<ScenarioRetroCessionLoss> ReadScenarioRetroCessionLossess(IDataReader reader)
        {
            while (reader.Read())
            {
                int index = 0;
                yield return new ScenarioRetroCessionLoss
                {
                    ScenarioId = reader.GetInt64(index),
                    EventKey = reader.GetString(++index),
                    RetroAllocationId = reader.GetInt64(++index),
                    LayerId = reader.GetInt64(++index),
                    RetroInvestorId = reader.GetInt64(++index),
                    Currency = reader.GetString(++index),
                    Loss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    OccLoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                    ReinstPremium = reader.IsDBNull(++index) ? null : reader.GetDecimal(index)
                };
            }
        }

        #endregion Methods
    }
}
