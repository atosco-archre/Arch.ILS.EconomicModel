using Studio.Core.Sql;
using System;
using System.Data;
using System.Data.SqlClient;

namespace Arch.ILS.EconomicModel.Historical
{
    public class ScenarioSqlRepository : SqlRepository
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
      ,LayerId
      ,RegisMKey
      ,SubmissionId
      ,LayerNum
      ,SubLayerNum
      ,ReinstCount
      ,Placement
      ,OccLimit
      ,OccRetention
      ,CascadeRetention
      ,AAD
      ,AggLimit
      ,AggRetention
      ,Franchise
      ,FranchiseReverse
      ,RiskLimit
      ,Inception
      ,UWYear
      ,Expiration
      ,ExpirationFinal
      ,Facility
      ,Segment
      ,LOB
      ,ContractType
      ,LimitBasis
      ,AttachBasis
      ,LAETerm
      ,ROL
      ,QuoteROL
      ,EstimatedShare
      ,SignedShare
      ,AuthShare
      ,QuotedShare
      ,Status
      ,LayerDesc
      ,Notes
      ,Rate
      ,PremiumFreq
      ,LayerType
      ,CreateDate
      ,IsActive
      ,IsDeleted
      ,InuringLimit
      ,RiskRetention
      ,LayerCategory
      ,LayerCatalog
      ,Premium
      ,QuotePremium
      ,RelShare
      ,TargetNetShare
      ,BoundFXRate
      ,BoundFXDate
      ,ERCActual
      ,ERCActualSource
      ,ELMarketShare
      ,ELHistoricalBurn
      ,ELBroker
      ,ProgramId
      ,ProgramExtName
      ,CedentId
      ,CedentName
      ,CedentGroupId
      ,CedentGroupName
      ,DepartmentId
      ,DepartmentName
      ,OfficeId
      ,OfficeName
      ,CompanyId
      ,CompanyName
      ,UnderwriterId
      ,Underwriter
      ,UnderwriterName
      ,SubmissionTranType
      ,Currency
      ,LegalEntity
      ,ModifiedDate
      ,ModifiedBy
      ,ReinstPercent
      ,CompanyCode
      ,RiskRegion
      ,RiskZone
  FROM dbo.ScenarioLayer";

        private const string GET_SCENARIOLAYERBYSCENARIOID = @"SELECT  ScenarioId
      ,LayerId
      ,RegisMKey
      ,SubmissionId
      ,LayerNum
      ,SubLayerNum
      ,ReinstCount
      ,Placement
      ,OccLimit
      ,OccRetention
      ,CascadeRetention
      ,AAD
      ,AggLimit
      ,AggRetention
      ,Franchise
      ,FranchiseReverse
      ,RiskLimit
      ,Inception
      ,UWYear
      ,Expiration
      ,ExpirationFinal
      ,Facility
      ,Segment
      ,LOB
      ,ContractType
      ,LimitBasis
      ,AttachBasis
      ,LAETerm
      ,ROL
      ,QuoteROL
      ,EstimatedShare
      ,SignedShare
      ,AuthShare
      ,QuotedShare
      ,Status
      ,LayerDesc
      ,Notes
      ,Rate
      ,PremiumFreq
      ,LayerType
      ,CreateDate
      ,IsActive
      ,IsDeleted
      ,InuringLimit
      ,RiskRetention
      ,LayerCategory
      ,LayerCatalog
      ,Premium
      ,QuotePremium
      ,RelShare
      ,TargetNetShare
      ,BoundFXRate
      ,BoundFXDate
      ,ERCActual
      ,ERCActualSource
      ,ELMarketShare
      ,ELHistoricalBurn
      ,ELBroker
      ,ProgramId
      ,ProgramExtName
      ,CedentId
      ,CedentName
      ,CedentGroupId
      ,CedentGroupName
      ,DepartmentId
      ,DepartmentName
      ,OfficeId
      ,OfficeName
      ,CompanyId
      ,CompanyName
      ,UnderwriterId
      ,Underwriter
      ,UnderwriterName
      ,SubmissionTranType
      ,Currency
      ,LegalEntity
      ,ModifiedDate
      ,ModifiedBy
      ,ReinstPercent
      ,CompanyCode
      ,RiskRegion
      ,RiskZone
  FROM dbo.ScenarioLayer
 WHERE ScenarioId = {0}";

        private const string GET_SCENARIOLAYERBYLAYERID = @"SELECT  ScenarioId
      ,LayerId
      ,RegisMKey
      ,SubmissionId
      ,LayerNum
      ,SubLayerNum
      ,ReinstCount
      ,Placement
      ,OccLimit
      ,OccRetention
      ,CascadeRetention
      ,AAD
      ,AggLimit
      ,AggRetention
      ,Franchise
      ,FranchiseReverse
      ,RiskLimit
      ,Inception
      ,UWYear
      ,Expiration
      ,ExpirationFinal
      ,Facility
      ,Segment
      ,LOB
      ,ContractType
      ,LimitBasis
      ,AttachBasis
      ,LAETerm
      ,ROL
      ,QuoteROL
      ,EstimatedShare
      ,SignedShare
      ,AuthShare
      ,QuotedShare
      ,Status
      ,LayerDesc
      ,Notes
      ,Rate
      ,PremiumFreq
      ,LayerType
      ,CreateDate
      ,IsActive
      ,IsDeleted
      ,InuringLimit
      ,RiskRetention
      ,LayerCategory
      ,LayerCatalog
      ,Premium
      ,QuotePremium
      ,RelShare
      ,TargetNetShare
      ,BoundFXRate
      ,BoundFXDate
      ,ERCActual
      ,ERCActualSource
      ,ELMarketShare
      ,ELHistoricalBurn
      ,ELBroker
      ,ProgramId
      ,ProgramExtName
      ,CedentId
      ,CedentName
      ,CedentGroupId
      ,CedentGroupName
      ,DepartmentId
      ,DepartmentName
      ,OfficeId
      ,OfficeName
      ,CompanyId
      ,CompanyName
      ,UnderwriterId
      ,Underwriter
      ,UnderwriterName
      ,SubmissionTranType
      ,Currency
      ,LegalEntity
      ,ModifiedDate
      ,ModifiedBy
      ,ReinstPercent
      ,CompanyCode
      ,RiskRegion
      ,RiskZone
  FROM dbo.ScenarioLayer
 WHERE LayerId = {0}";

        private const string GET_SCENARIOLAYERLOSS = @"SELECT ScenarioId
      ,EventKey
      ,LayerId
      ,SubmissionId
      ,ProgramId
      ,CedentId
      ,IsFHCF
      ,LAE
      ,LossCurrency
      ,FXRateToLayerCurrency
      ,LayerCurrency
      ,GULoss
      ,GULossWithLAE
      ,LayerLoss
      ,GrossLoss
      ,NetLoss
      ,OccGrossLoss
      ,OccNetLoss
      ,SectionsAdjustment
      ,LastCumulativeOccLoss
      ,LastAggLoss
      ,OccLoss
      ,AggLoss
      ,NewAggLimit
      ,NewAggRetention
      ,NewAggLimitInLayerCurrency
      ,NewAggRetentionInLayerCurrency
      ,CalculationStatus
      ,GrossTranType
      ,NetTranType
      ,TotalNetRetroCession
      ,GULossBasedOnAdjustedMKS
      ,GULossBasedOnMKS
      ,GULossOverridden
      ,CededReinstPremium
      ,GrossReinstPremium
      ,LimitUsed
      ,LossCeded
      ,NetReinstPremium
      ,OccLossCeded
      ,ReinstPremium
      ,MarketShare
      ,CompanyCode
      ,RiskRegion
      ,RiskZone
  FROM dbo.ScenarioLayerLoss";

        private const string GET_SCENARIOLAYERLOSSBYSCENARIOID = @"SELECT ScenarioId
      ,EventKey
      ,LayerId
      ,SubmissionId
      ,ProgramId
      ,CedentId
      ,IsFHCF
      ,LAE
      ,LossCurrency
      ,FXRateToLayerCurrency
      ,LayerCurrency
      ,GULoss
      ,GULossWithLAE
      ,LayerLoss
      ,GrossLoss
      ,NetLoss
      ,OccGrossLoss
      ,OccNetLoss
      ,SectionsAdjustment
      ,LastCumulativeOccLoss
      ,LastAggLoss
      ,OccLoss
      ,AggLoss
      ,NewAggLimit
      ,NewAggRetention
      ,NewAggLimitInLayerCurrency
      ,NewAggRetentionInLayerCurrency
      ,CalculationStatus
      ,GrossTranType
      ,NetTranType
      ,TotalNetRetroCession
      ,GULossBasedOnAdjustedMKS
      ,GULossBasedOnMKS
      ,GULossOverridden
      ,CededReinstPremium
      ,GrossReinstPremium
      ,LimitUsed
      ,LossCeded
      ,NetReinstPremium
      ,OccLossCeded
      ,ReinstPremium
      ,MarketShare
      ,CompanyCode
      ,RiskRegion
      ,RiskZone
  FROM dbo.ScenarioLayerLoss
 WHERE ScenarioId = {0}";

        private const string GET_SCENARIOLAYERLOSSBYLAYERID = @"SELECT ScenarioId
      ,EventKey
      ,LayerId
      ,SubmissionId
      ,ProgramId
      ,CedentId
      ,IsFHCF
      ,LAE
      ,LossCurrency
      ,FXRateToLayerCurrency
      ,LayerCurrency
      ,GULoss
      ,GULossWithLAE
      ,LayerLoss
      ,GrossLoss
      ,NetLoss
      ,OccGrossLoss
      ,OccNetLoss
      ,SectionsAdjustment
      ,LastCumulativeOccLoss
      ,LastAggLoss
      ,OccLoss
      ,AggLoss
      ,NewAggLimit
      ,NewAggRetention
      ,NewAggLimitInLayerCurrency
      ,NewAggRetentionInLayerCurrency
      ,CalculationStatus
      ,GrossTranType
      ,NetTranType
      ,TotalNetRetroCession
      ,GULossBasedOnAdjustedMKS
      ,GULossBasedOnMKS
      ,GULossOverridden
      ,CededReinstPremium
      ,GrossReinstPremium
      ,LimitUsed
      ,LossCeded
      ,NetReinstPremium
      ,OccLossCeded
      ,ReinstPremium
      ,MarketShare
      ,CompanyCode
      ,RiskRegion
      ,RiskZone
  FROM dbo.ScenarioLayerLoss
 WHERE LayerId = {0}";

        private const string GET_SCENARIOOCCLAYERLOSS = @"SELECT ScenarioId
      ,EventKey
      ,LayerId
      ,SubmissionId
      ,ProgramId
      ,CedentId
      ,IsFHCF
      ,LAE
      ,LossCurrency
      ,FXRateToLayerCurrency
      ,LayerCurrency
      ,GULoss
      ,GULossWithLAE
      ,LayerLoss
      ,GrossLoss
      ,NetLoss
      ,OccGrossLoss
      ,OccNetLoss
      ,SectionsAdjustment
      ,LastCumulativeOccLoss
      ,LastAggLoss
      ,OccLoss
      ,AggLoss
      ,NewAggLimit
      ,NewAggRetention
      ,NewAggLimitInLayerCurrency
      ,NewAggRetentionInLayerCurrency
      ,CalculationStatus
      ,GrossTranType
      ,NetTranType
      ,TotalNetRetroCession
      ,GULossBasedOnAdjustedMKS
      ,GULossBasedOnMKS
      ,GULossOverridden
      ,CededReinstPremium
      ,GrossReinstPremium
      ,LimitUsed
      ,LossCeded
      ,NetReinstPremium
      ,OccLossCeded
      ,ReinstPremium
      ,MarketShare
      ,CompanyCode
      ,RiskRegion
      ,RiskZone
  FROM dbo.ScenarioLayerLoss
 WHERE (GULoss != 0 OR GrossLoss != 0 OR LayerLoss != 0)";

        private const string GET_SCENARIOLAYERSECTION = @"SELECT ScenarioId
      ,LayerId
      ,SectionId
      ,RollUpType
      ,FXRateToParent
  FROM dbo.ScenarioLayerSection";

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

        public ScenarioSqlRepository(string connectionString) : base(connectionString)
        {
        }

        public IEnumerable<Scenario> GetScenarios()
        {
            using (var reader = (SqlDataReader)ExecuteReaderSql(GET_SCENARIO))
            {
                while (reader.Read())
                {
                    int index = 0;
                    yield return new Scenario
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
                        InforceDate = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                        FXRatesDate = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
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
                }
            }
        }

        public IEnumerable<ScenarioLossEvent> GetScenarioLossEvents()
        {
            using (var reader = (SqlDataReader)ExecuteReaderSql(GET_SCENARIOLOSSEVENT))
            {
                while (reader.Read())
                {
                    int index = 0;
                    yield return new ScenarioLossEvent
                    {
                        ScenarioId = reader.GetInt64(index),
                        EventKey = reader.GetString(++index),
                        Name = reader.GetString(++index),
                        AutoName = reader.IsDBNull(++index) ? null : reader.GetString(index),
                        EventDate = reader.GetDateTime(++index),
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
                }
            }
        }

        public IEnumerable<ScenarioLayer> GetScenarioLayers()
        {
            using (var reader = (SqlDataReader)ExecuteReaderSql(GET_SCENARIOLAYER))
            {
                while (reader.Read())
                {
                    yield return ReadScenarioLayer(reader);
                }
            }
        }

        public IEnumerable<ScenarioLayer> GetScenarioLayersByScenarioId(long scenarioId)
        {
            using (var reader = (SqlDataReader)ExecuteReaderSql(string.Format(GET_SCENARIOLAYERBYSCENARIOID, scenarioId)))
            {
                while (reader.Read())
                {
                    yield return ReadScenarioLayer(reader);
                }
            }
        }

        public IEnumerable<ScenarioLayer> GetScenarioLayersByLayerId(long layerId)
        {
            using (var reader = (SqlDataReader)ExecuteReaderSql(string.Format(GET_SCENARIOLAYERBYLAYERID, layerId)))
            {
                while (reader.Read())
                {
                    yield return ReadScenarioLayer(reader);
                }
            }
        }

        private ScenarioLayer ReadScenarioLayer(IDataReader reader)
        {
            int index = 0;
            return new ScenarioLayer
            {
                ScenarioId = reader.GetInt64(index),
                LayerId = reader.GetInt64(++index),
                RegisMKey = reader.IsDBNull(++index) ? null : reader.GetString(index),
                SubmissionId = reader.GetInt64(++index),
                LayerNum = reader.GetInt32(++index),
                SubLayerNum = reader.GetInt32(++index),
                ReinstCount = reader.GetInt32(++index),
                Placement = reader.GetDecimal(++index),
                OccLimit = reader.GetDecimal(++index),
                OccRetention = reader.GetDecimal(++index),
                CascadeRetention = reader.GetDecimal(++index),
                AAD = reader.GetDecimal(++index),
                AggLimit = reader.GetDecimal(++index),
                AggRetention = reader.GetDecimal(++index),
                Franchise = reader.GetDecimal(++index),
                FranchiseReverse = reader.GetDecimal(++index),
                RiskLimit = reader.GetDecimal(++index),
                Inception = reader.GetDateTime(++index),
                UWYear = reader.GetInt32(++index),
                Expiration = reader.GetDateTime(++index),
                ExpirationFinal = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                Facility = reader.IsDBNull(++index) ? null : reader.GetString(index),
                Segment = reader.IsDBNull(++index) ? null : reader.GetString(index),
                LOB = reader.IsDBNull(++index) ? null : reader.GetString(index),
                ContractType = reader.GetInt32(++index),
                LimitBasis = reader.GetInt32(++index),
                AttachBasis = reader.GetInt32(++index),
                LAETerm = reader.GetInt32(++index),
                ROL = reader.GetDecimal(++index),
                QuoteROL = reader.GetDecimal(++index),
                EstimatedShare = reader.GetDecimal(++index),
                SignedShare = reader.GetDecimal(++index),
                AuthShare = reader.GetDecimal(++index),
                QuotedShare = reader.GetDecimal(++index),
                Status = reader.GetInt32(++index),
                LayerDesc = reader.IsDBNull(++index) ? null : reader.GetString(index),
                Notes = reader.IsDBNull(++index) ? null : reader.GetString(index),
                Rate = reader.GetDecimal(++index),
                PremiumFreq = reader.GetInt32(++index),
                LayerType = reader.GetInt32(++index),
                CreateDate = reader.GetDateTime(++index),
                IsActive = reader.GetBoolean(++index),
                IsDeleted = reader.GetBoolean(++index),
                InuringLimit = reader.GetDecimal(++index),
                RiskRetention = reader.GetDecimal(++index),
                LayerCategory = reader.GetInt32(++index),
                LayerCatalog = reader.IsDBNull(++index) ? null : reader.GetString(index),
                Premium = reader.GetDecimal(++index),
                QuotePremium = reader.GetDecimal(++index),
                RelShare = reader.GetDecimal(++index),
                TargetNetShare = reader.GetDecimal(++index),
                BoundFXRate = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                BoundFXDate = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                ERCActual = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                ERCActualSource = reader.IsDBNull(++index) ? null : reader.GetString(index),
                ELMarketShare = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                ELHistoricalBurn = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                ELBroker = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                ProgramId = reader.GetInt64(++index),
                ProgramExtName = reader.IsDBNull(++index) ? null : reader.GetString(index),
                CedentId = reader.GetInt64(++index),
                CedentName = reader.IsDBNull(++index) ? null : reader.GetString(index),
                CedentGroupId = reader.GetInt64(++index),
                CedentGroupName = reader.IsDBNull(++index) ? null : reader.GetString(index),
                DepartmentId = reader.GetInt64(++index),
                DepartmentName = reader.IsDBNull(++index) ? null : reader.GetString(index),
                OfficeId = reader.GetInt64(++index),
                OfficeName = reader.IsDBNull(++index) ? null : reader.GetString(index),
                CompanyId = reader.GetInt64(++index),
                CompanyName = reader.IsDBNull(++index) ? null : reader.GetString(index),
                UnderwriterId = reader.GetInt64(++index),
                Underwriter = reader.IsDBNull(++index) ? null : reader.GetString(index),
                UnderwriterName = reader.IsDBNull(++index) ? null : reader.GetString(index),
                SubmissionTranType = reader.GetInt32(++index),
                Currency = reader.IsDBNull(++index) ? null : reader.GetString(index),
                LegalEntity = reader.IsDBNull(++index) ? null : reader.GetString(index),
                ModifiedDate = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                ModifiedBy = reader.IsDBNull(++index) ? null : reader.GetString(index),
                ReinstPercent = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                CompanyCode = reader.IsDBNull(++index) ? null : reader.GetInt64(index),
                RiskRegion = reader.IsDBNull(++index) ? null : reader.GetString(index),
                RiskZone = reader.IsDBNull(++index) ? null : reader.GetString(index),
            };
        }

        public IEnumerable<ScenarioLayerLoss> GetScenarioLayerLosses()
        {
            using (var reader = (SqlDataReader)ExecuteReaderSql(GET_SCENARIOLAYERLOSS))
            {
                while (reader.Read())
                {
                    yield return ReadScenarioLayerLoss(reader);
                }
            }
        }

        public IEnumerable<ScenarioLayerLoss> GetScenarioLayerLossesByScenarioId(long scenarioId)
        {
            using (var reader = (SqlDataReader)ExecuteReaderSql(string.Format(GET_SCENARIOLAYERLOSSBYSCENARIOID, scenarioId)))
            {
                while (reader.Read())
                {
                    yield return ReadScenarioLayerLoss(reader);
                }
            }
        }

        public IEnumerable<ScenarioLayerLoss> GetScenarioLayerLossesByLayerId(long layerId)
        {
            using (var reader = (SqlDataReader)ExecuteReaderSql(string.Format(GET_SCENARIOLAYERLOSSBYLAYERID, layerId)))
            {
                while (reader.Read())
                {
                    yield return ReadScenarioLayerLoss(reader);
                }
            }
        }

        public IEnumerable<ScenarioLayerLoss> GetScenarioOccLayerLosses()
        {
            using (var reader = (SqlDataReader)ExecuteReaderSql(GET_SCENARIOOCCLAYERLOSS))
            {
                while (reader.Read())
                {
                    yield return ReadScenarioLayerLoss(reader);
                }
            }
        }

        private ScenarioLayerLoss ReadScenarioLayerLoss(IDataReader reader)
        {
            int index = 0;
            return new ScenarioLayerLoss
            {
                ScenarioId = reader.GetInt64(index),
                EventKey = reader.GetString(++index),
                LayerId = reader.GetInt64(++index),
                SubmissionId = reader.GetInt64(++index),
                ProgramId = reader.GetInt64(++index),
                CedentId = reader.GetInt64(++index),
                IsFHCF = reader.GetBoolean(++index),
                LAE = reader.GetDecimal(++index),
                LossCurrency = reader.IsDBNull(++index) ? null : reader.GetString(index),
                FXRateToLayerCurrency = reader.GetDecimal(++index),
                LayerCurrency = reader.IsDBNull(++index) ? null : reader.GetString(index),
                GULoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                GULossWithLAE = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                LayerLoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                GrossLoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                NetLoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                OccGrossLoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                OccNetLoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                SectionsAdjustment = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                LastCumulativeOccLoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                LastAggLoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                OccLoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                AggLoss = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                NewAggLimit = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                NewAggRetention = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                NewAggLimitInLayerCurrency = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                NewAggRetentionInLayerCurrency = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                CalculationStatus = reader.GetInt32(++index),
                GrossTranType = reader.GetInt32(++index),
                NetTranType = reader.GetInt32(++index),
                TotalNetRetroCession = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                GULossBasedOnAdjustedMKS = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                GULossBasedOnMKS = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                GULossOverridden = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                CededReinstPremium = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                GrossReinstPremium = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                LimitUsed = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                LossCeded = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                NetReinstPremium = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                OccLossCeded = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                ReinstPremium = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                MarketShare = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                CompanyCode = reader.IsDBNull(++index) ? null : reader.GetInt64(index),
                RiskRegion = reader.IsDBNull(++index) ? null : reader.GetString(index),
                RiskZone = reader.IsDBNull(++index) ? null : reader.GetString(index)
            };
        }

        public IEnumerable<ScenarioLayerSection> GetScenarioLayerSections()
        {
            using (var reader = (SqlDataReader)ExecuteReaderSql(GET_SCENARIOLAYERSECTION))
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
        }

        public IEnumerable<ScenarioRetroCession> GetScenarioRetroCessions()
        {
            using (var reader = (SqlDataReader)ExecuteReaderSql(GET_SCENARIORETROCESSION))
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
                        Expiration = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                        Inception = reader.IsDBNull(++index) ? null : reader.GetDateTime(index)
                    };
                }
            }
        }

        public IEnumerable<ScenarioRetroCessionLoss> GetScenarioRetroCessionLosses()
        {
            using (var reader = (SqlDataReader)ExecuteReaderSql(GET_SCENARIORETROCESSIONLOSS))
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
        }
    }
}
