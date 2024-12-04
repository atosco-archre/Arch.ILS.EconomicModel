
using System;
using System.Buffers;
using System.Data.SqlClient;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

using Arch.ILS.Core;
using Studio.Core;
using Studio.Core.Sql;

namespace Arch.ILS.EconomicModel
{
    public class RevoRepository : IRevoRepository
    {
        #region Variables

        private readonly IRepository _repository;

        #endregion Variables

        #region Constructors

        public RevoRepository(IRepository repository)
        {
            _repository = repository;
        }

        #endregion Constructors

        #region Methods

        #region Layer Info

        public Task<Dictionary<int, Layer>> GetLayers()
        {
            return Task.Factory.StartNew(() =>
            {
                return _repository.ExecuteReaderSql(GET_LAYERS).GetObjects<Layer>().ToDictionary(x => x.LayerId);
            });
        }

        public Task<Dictionary<int, LayerDetail>> GetLayerDetails()
        {
            return Task.Factory.StartNew(() =>
            {
                Dictionary<int, LayerDetail> layerDetails = new Dictionary<int, LayerDetail>();
                using (var reader = _repository.ExecuteReaderSql(Translate(GET_LAYER_DETAILS)))
                {
                    while (reader.Read())
                    {
                        int index = 0;
                        LayerDetail layerDetail = new LayerDetail
                        {
                            LayerId = reader.GetInt32(index),
                            SubmissionId = reader.GetInt32(++index),
                            //LayerNum = reader.GetInt32(++index),
                            //SubLayerNum = reader.GetInt32(++index),
                            //ReinstCount = reader.GetInt32(++index),
                            Placement = reader.GetDecimal(++index),
                            OccLimit = reader.GetDecimal(++index),
                            //OccRetention = reader.GetDecimal(++index),
                            //CascadeRetention = reader.GetDecimal(++index),
                            //AAD = reader.GetDecimal(++index),
                            //Var1Retention = reader.GetDecimal(++index),
                            //Var2Retention = reader.GetDecimal(++index),
                            AggLimit = reader.GetDecimal(++index),
                            //AggRetention = reader.GetDecimal(++index),
                            //Franchise = reader.GetDecimal(++index),
                            //FranchiseReverse = reader.GetDecimal(++index),
                            RiskLimit = reader.GetDecimal(++index),
                            Inception = reader.GetDateTime(++index),
                            UWYear = reader.GetInt32(++index),
                            Expiration = reader.GetDateTime(++index),
                            //ExpirationFinal = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                            //Facility = reader.GetString(++index),
                            //Segment = reader.GetString(++index),
                            //LOB = reader.GetString(++index),
                            //ContractType = reader.GetInt32(++index),
                            LimitBasis = (LimitBasis)reader.GetInt32(++index),
                            //AttachBasis = reader.GetInt32(++index),
                            //LAETerm = reader.GetInt32(++index),
                            //LossTrigger = reader.GetInt32(++index),
                            //ROL = reader.GetDecimal(++index),
                            //QuoteROL = reader.GetDecimal(++index),
                            //ERC = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //ERCModel = reader.GetDecimal(++index),
                            //ERCMid = reader.GetDecimal(++index),
                            //ERCPareto = reader.GetDecimal(++index),
                            //RegisId = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //RegisNbr = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //RegisMKey = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //RegisIdCt = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //RegisNbrCt = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //BurnReported = reader.GetDecimal(++index),
                            //BurnTrended = reader.GetDecimal(++index),
                            //YearPeriodSelected = reader.GetInt32(++index),
                            //YearPeriodLoss = reader.GetInt32(++index),
                            //CatLoss1 = reader.GetDecimal(++index),
                            //CatLoss2 = reader.GetDecimal(++index),
                            //CatLoss3 = reader.GetDecimal(++index),
                            EstimatedShare = reader.GetDecimal(++index),
                            SignedShare = reader.GetDecimal(++index),
                            //AuthShare = reader.GetDecimal(++index),
                            //QuotedShare = reader.GetDecimal(++index),
                            Status = (ContractStatus)reader.GetByte(++index),
                            //LayerDesc = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //Notes = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //RegisMsg = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //ExpiringLayerId = reader.IsDBNull(++index) ? null : reader.GetInt32(index),
                            //Commission = reader.GetDecimal(++index),
                            //CommOverride = reader.GetDecimal(++index),
                            //Brokerage = reader.GetDecimal(++index),
                            //Tax = reader.GetDecimal(++index),
                            //OtherExpenses = reader.GetDecimal(++index),
                            //IsVarComm = reader.GetBoolean(++index),
                            //VarCommHi = reader.GetDecimal(++index),
                            //VarCommLow = reader.GetDecimal(++index),
                            //IsGrossUpComm = reader.GetBoolean(++index),
                            //GrossUpFactor = reader.GetDecimal(++index),
                            //IsSlidingScale = reader.GetBoolean(++index),
                            //SSCommProv = reader.GetDecimal(++index),
                            //SSCommMax = reader.GetDecimal(++index),
                            //SSCommMin = reader.GetDecimal(++index),
                            //SSLossRatioProv = reader.GetDecimal(++index),
                            //SSLossRatioMax = reader.GetDecimal(++index),
                            //SSLossRatioMin = reader.GetDecimal(++index),
                            //IsProfitComm = reader.GetBoolean(++index),
                            //ProfitComm = reader.GetDecimal(++index),
                            //CCFYears = reader.GetInt32(++index),
                            //DCFYears = reader.GetInt32(++index),
                            //DCFAmount = reader.GetInt32(++index),
                            //PCStartDate = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                            //ComAccountProtect = reader.GetDecimal(++index),
                            //ProfitCommissionExpAllowance = reader.GetDecimal(++index),
                            //Rate = reader.GetDecimal(++index),
                            //PremiumFreq = reader.GetInt32(++index),
                            //AdjustmentBaseType = reader.GetInt32(++index),
                            //LayerType = reader.GetInt32(++index),
                            //FHCFBand = reader.GetInt32(++index),
                            //CreateDate = reader.GetDateTime(++index),
                            //CreateUser = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //ModifyDate = reader.GetDateTime(++index),
                            //ModifyUser = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //IsActive = reader.GetBoolean(++index),
                            //IsDeleted = reader.GetBoolean(++index),
                            //TopUpZoneId = reader.IsDBNull(++index) ? null : reader.GetInt32(index),
                            //ERCQuote = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //DeclineReason = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //InuringLimit = reader.GetDecimal(++index),
                            //RiskRetention = reader.GetDecimal(++index),
                            //ReinsurerExpenses = reader.GetDecimal(++index),
                            //LayerCategory = reader.GetInt32(++index),
                            //LayerCatalog = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            Premium = reader.GetDecimal(++index),
                            //QuotePremium = reader.GetDecimal(++index),
                            //RiskZoneId = reader.IsDBNull(++index) ? null : reader.GetInt32(index),
                            //RelShare = reader.GetDecimal(++index),
                            //TargetNetShare = reader.GetDecimal(++index),
                            //RegisLayerCode = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //SnpLobId = reader.IsDBNull(++index) ? null : reader.GetInt32(index),
                            //InvestmentReturn = reader.GetDecimal(++index),
                            //NonCatMarginAllowance = reader.GetDecimal(++index),
                            //LossDuration = reader.GetDecimal(++index),
                            //DiversificationFactor = reader.GetDecimal(++index),
                            //EarningType = reader.GetInt32(++index),
                            //SourceId = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //OrderPct = reader.GetDecimal(++index),
                            //BrokerRef = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //AcctBrokerId = reader.IsDBNull(++index) ? null : reader.GetInt32(index),
                            //IsAdditionalPremium = reader.GetBoolean(++index),
                            //IsCommonAcct = reader.GetBoolean(++index),
                            //EventNumber = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //IsStopLoss = reader.GetBoolean(++index),
                            //StopLossLimitPct = reader.GetDecimal(++index),
                            //StopLossAttachPct = reader.GetDecimal(++index),
                            //IsLossCorridor = reader.GetBoolean(++index),
                            //LossCorridorBeginPct = reader.GetDecimal(++index),
                            //LossCorridorEndPct = reader.GetDecimal(++index),
                            //LossCorridorCedePct = reader.GetDecimal(++index),
                            //OccLimitInPct = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //OccRetnInPct = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //ExpiringCorreShare = reader.GetDecimal(++index),
                            //CorreAuthMin = reader.GetDecimal(++index),
                            //CorreAuthTarget = reader.GetDecimal(++index),
                            //CorreAuthMax = reader.GetDecimal(++index),
                            //CorreRenewalMin = reader.GetDecimal(++index),
                            //SharedToCorre = reader.GetInt32(++index),
                            //SignedCorreShare = reader.GetDecimal(++index),
                            //QuotedCorreShare = reader.GetDecimal(++index),
                            //AuthCorreShare = reader.GetDecimal(++index),
                            //FrontingFee = reader.GetDecimal(++index),
                            RowVersion = reader.GetInt64(++index),
                            //NonCatWeightPC = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //NonCatWeightSS = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            BoundFXRate = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            BoundFXDate = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                            //RegisStatus = reader.GetInt32(++index),
                            //IsDifferentialTerms = reader.GetBoolean(++index),
                            //RolRpp = reader.GetDecimal(++index),
                            //WILResolution = reader.GetInt32(++index),
                            //IsParametric = reader.GetBoolean(++index),
                            //PricingSource = reader.GetInt32(++index),
                            //IRISPolicySeqNumber = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //IRISStatus = reader.GetInt32(++index),
                            //IRISComments = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //IRISRefId = reader.GetInt32(++index),
                            //IRISClassCode = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //IRISBranchCode = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //IRISTradeCode = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //IRISPlacingCode = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //ExpectedGrossNetPremiumGBP = reader.GetDouble(++index),
                            //IRISProductCode = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //StopLossBufferPct = reader.GetDecimal(++index),
                            //ERCActual = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //ERCActualSource = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //ELMarketShare = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //ELHistoricalBurn = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //ELBroker = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //MAOL = reader.IsDBNull(++index) ? null : reader.GetInt32(index),
                            //NCBR = reader.GetBoolean(++index),
                            //IsTerrorismSubLimitAppl = reader.GetBoolean(++index),
                            //TerrorismSubLimit = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //TerrorismSubLimitComments = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //LloydsCapital = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //LloydsROC = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //QuoteExpire = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                            //AuthExpire = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                            //MktROL = reader.GetDecimal(++index),
                            //IsHidden = reader.GetBoolean(++index),
                            //Cloud = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //Ransom = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //BudgetROL = reader.GetDecimal(++index),
                            //BudgetPremium = reader.GetDecimal(++index),
                            //BudgetShare = reader.GetDecimal(++index),
                        };

                        layerDetails[layerDetail.LayerId] = layerDetail;
                    }
                }

                return layerDetails;
            });
        }

        public Task<Dictionary<int, LayerMetaInfo>> GetLayerMetaInfos()
        {
            return Task.Factory.StartNew(() =>
            {
                Dictionary<int, LayerMetaInfo> layerMetaInfos = new Dictionary<int, LayerMetaInfo>();
                using (var reader = _repository.ExecuteReaderSql(Translate(GET_LAYER_META_INFOS)))
                {
                    while (reader.Read())
                    {
                        int index = 0;
                        int layerId = reader.GetInt32(index);
                        layerMetaInfos[layerId] = new LayerMetaInfo
                        { 
                            LayerId  = layerId,
                            Segment = Enum.Parse<SegmentType>(reader.GetString(++index))
                        };
                    }
                }

                return layerMetaInfos;
            });
        }

        public Task<IEnumerable<Reinstatement>> GetLayerReinstatements()
        {
            return Task.Factory.StartNew(() =>
            {
                return _repository.ExecuteReaderSql(Translate(GET_LAYER_REINSTATEMENTS)).GetObjects<Reinstatement>();
            });
        }

        public Task<IEnumerable<LayerTopUpZone>> GetLayerTopUpZones()
        {
            return Task.Factory.StartNew(() =>
            {
                return _repository.ExecuteReaderSql(GET_LAYER_TOPUPZONE).GetObjects<LayerTopUpZone>();
            });
        }

        public Task<IDictionary<int, Submission>> GetSubmissions()
        {
            return Task.Factory.StartNew(() =>
            {
                IDictionary<int, Submission> submissions = new Dictionary<int, Submission>();
                using (var reader = _repository.ExecuteReaderSql(Translate(GET_SUBMISSIONS)))
                {
                    while (reader.Read())
                    {
                        int index = 0;
                        Submission submission = new()
                        {
                            SubmissionId = reader.GetInt32(index),
                            ProgramId = reader.GetInt32(++index),
                            RegisId = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            BaseCurrency = reader.GetString(++index),
                            Currency = reader.GetString(++index),
                            FXRate = reader.GetDecimal(++index),
                            FXDate = reader.GetDateTime(++index),
                            TranType = (TranType)(byte)reader.GetInt32(++index),
                            InceptionDefault = reader.GetDateTime(++index),
                            UWYearDefault = reader.GetInt32(++index),
                            IsMultiyear = reader.GetBoolean(++index),
                            IsCancellable = reader.GetBoolean(++index),
                            //ExpirationDefault = reader.GetDateTime(++index),
                            //QuoteDeadline = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                            //AuthDeadline = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                            //Arrived = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                            //BrokerId = reader.GetInt32(++index),
                            //BrokerContactId = reader.IsDBNull(++index) ? null : reader.GetInt32(index),
                            //UnderwriterId = reader.GetInt32(++index),
                            //ActuaryId = reader.IsDBNull(++index) ? null : reader.GetInt32(index),
                            //AnalystId = reader.IsDBNull(++index) ? null : reader.GetInt32(index),
                            //ModelerId = reader.IsDBNull(++index) ? null : reader.GetInt32(index),
                            //RiskZoneId = reader.IsDBNull(++index) ? null : reader.GetInt32(index),
                            //Notes = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //UWNotes = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //Correspondence = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //StrategicNotes = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //RefId = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //Status = reader.GetInt32(++index),
                            //IsRenewal = reader.GetBoolean(++index),
                            //DocStatus = reader.GetInt32(++index),
                            //ModelingStatus = reader.GetInt32(++index),
                            //Priority = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //ExpiringSubmissionId = reader.IsDBNull(++index) ? null : reader.GetInt32(index),
                            //Surplus = reader.GetDecimal(++index),
                            //ClientScore = reader.GetInt32(++index),
                            //SubmissionWriteupId = reader.GetInt32(++index),
                            //CreateDate = reader.GetDateTime(++index),
                            //CreateUser = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //ModifyDate = reader.GetDateTime(++index),
                            //ModifyUser = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //IsActive = reader.GetBoolean(++index),
                            //IsDeleted = reader.GetBoolean(++index),
                            //LegalTermsId = reader.IsDBNull(++index) ? null : reader.GetInt32(index),
                            //PlacementYear = reader.GetInt32(++index),
                            //ParentSubmissionId = reader.IsDBNull(++index) ? null : reader.GetInt32(index),
                            //CedentAltName = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //ModelingDeadline = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                            //ModelingNotes = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //DataLinkNotes = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //MdlStatusDate = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                            //ActuarialNotes = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //RelshipUnderwriterId = reader.IsDBNull(++index) ? null : reader.GetInt32(index),
                            //MarketShare = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //CorreAuthDeadline = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                            //CorreStatus = reader.GetInt32(++index),
                            //ActuarialStatus = reader.GetInt32(++index),
                            //SubmissionDataLinkNotes = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //ActuarialDataLinkNotes = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //ActuarialDeadline = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                            //Source = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //IsCorreInterest = reader.GetBoolean(++index),
                            //ActuarialPriority = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //RegisSyncStatus = reader.GetInt32(++index),
                            //LastRegisSyncByUserId = reader.IsDBNull(++index) ? null : reader.GetInt32(index),
                            //LastRegisSyncDate = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                            //ModelingComplexity = reader.GetInt32(++index),
                            //ActuarialDataCheck = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //ActuarialRanking = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //IsActuarialDataCheckRequested = reader.GetBoolean(++index),
                            RowVersion = reader.GetInt64(++index),
                            //ERCLossViewArch = reader.GetInt32(++index),
                            //ERCLossViewAir = reader.GetInt32(++index),
                            //ERCLossViewRMS = reader.GetInt32(++index),
                            //FxRateSBFUSD = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //FxRateSBFGBP = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //FxDateSBF = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                            //IrisPolicyNumber = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //RationaleQuote = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //RationaleAuth = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //RationaleSigned = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //IrisSLA = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                            //IsCollateralized = reader.GetBoolean(++index),
                            //BrokerRating = reader.GetInt32(++index),
                            //BrokerRationale = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //PNOCDays = reader.GetInt32(++index),
                            //ClientAdvocacyRating = reader.GetInt32(++index),
                            //ClientAdvocacyRationale = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //LMXIndicator = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //ActuaryPeerReviewerId = reader.IsDBNull(++index) ? null : reader.GetInt32(index),
                            //ClientAdvocacyLink = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //GroupBuyerId = reader.IsDBNull(++index) ? null : reader.GetInt32(index),
                            //LocalBuyerId = reader.IsDBNull(++index) ? null : reader.GetInt32(index),
                            //IsActuaryPeerReviewNotRequired = reader.GetBoolean(++index),
                            //CedeCoverageSelectionType = reader.GetInt32(++index),
                            //DataScoreRating = reader.GetInt32(++index),
                        };

                        submissions[submission.SubmissionId] = submission;
                    }
                }

                return submissions;
            });
        }

        #endregion LayerInfo

        #region Retro Info

        public Task<Dictionary<int, RetroProgram>> GetRetroPrograms()
        {
            return Task.Factory.StartNew(() =>
            {
                return _repository.ExecuteReaderSql(Translate(GET_RETRO_PROGRAM)).GetObjects<RetroProgram>().ToDictionary(x => x.RetroProgramId);
            });
        }

        public Task<IEnumerable<RetroInvestorReset>> GetRetroInvestorResets()
        {
            return Task.Factory.StartNew(() =>
            {
                return _repository.ExecuteReaderSql(Translate(GET_RETRO_INVESTOR_RESET)).GetObjects<RetroInvestorReset>();
            });
        }

        public Task<IEnumerable<RetroProgramReset>> GetRetroProgramResets()
        {
            return Task.Factory.StartNew(() =>
            {
                return _repository.ExecuteReaderSql(Translate(GET_RETRO_PROGRAM_RESET)).GetObjects<RetroProgramReset>();
            });
        }

        public Task<Dictionary<int, SPInsurer>> GetSPInsurers()
        {
            return Task.Factory.StartNew(() =>
            {
                Dictionary<int, SPInsurer> spInsurers = new Dictionary<int, SPInsurer>();
                using (var reader = _repository.ExecuteReaderSql(Translate(GET_SPINSURER)))
                {
                    while (reader.Read())
                    {
                        int index = 0;
                        SPInsurer spInsurer = new()
                        {
                            SPInsurerId = reader.GetInt32(index),
                            RetroProgramId = reader.GetInt32(++index),
                            //SegregatedAccount = reader.GetString(++index),
                            //ContractId = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //InsurerId = reader.GetInt32(++index),
                            //TrustBank = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //CreateDate = reader.GetDateTime(++index),
                            //CreateUser = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //ModifyDate = reader.GetDateTime(++index),
                            //ModifyUser = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //IsActive = reader.GetBoolean(++index),
                            //IsDeleted = reader.GetBoolean(++index),
                            RowVersion = reader.GetInt64(++index),
                            //TrustAccountNumber = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //FundsWithheldAccountNumber = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            InitialCommutationDate = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                            FinalCommutationDate = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                        };

                        spInsurers[spInsurer.SPInsurerId] = spInsurer;
                    }
                }

                return spInsurers;
            });
        }

        public Task<IList<RetroInvestor>> GetRetroInvestors()
        {
            return Task.Factory.StartNew(() =>
            {
                IList<RetroInvestor> retroInvestors = new List<RetroInvestor>();
                using (var reader = _repository.ExecuteReaderSql(Translate(GET_RETRO_INVESTOR)))
                {
                    while (reader.Read())
                    {
                        int index = 0;
                        RetroInvestor retroInvestor = new()
                        {
                            RetroInvestorId = reader.GetInt32(index),
                            SPInsurerId = reader.GetInt32(++index),
                            //Name = reader.GetString(++index),
                            Status = reader.GetInt32(++index),
                            TargetCollateral = reader.GetDecimal(++index),
                            NotionalCollateral = reader.GetDecimal(++index),
                            InvestmentEstimated = reader.GetDecimal(++index),
                            InvestmentAuth = reader.GetDecimal(++index),
                            InvestmentSigned = reader.GetDecimal(++index),
                            InvestmentEstimatedAmt = reader.GetDecimal(++index),
                            InvestmentAuthAmt = reader.GetDecimal(++index),
                            InvestmentSignedAmt = reader.GetDecimal(++index),
                            ExcludedFacilities = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            ExcludedLayerSubNos = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            ExcludedDomiciles = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            IsFundsWithheld = reader.GetBoolean(++index),
                            RetroCommissionId = reader.GetInt32(++index),
                            //CreateDate = reader.GetDateTime(++index),
                            //CreateUser = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //ModifyDate = reader.GetDateTime(++index),
                            //ModifyUser = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //IsActive = reader.GetBoolean(++index),
                            //IsDeleted = reader.GetBoolean(++index),
                            //RuleDefs = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            RowVersion = reader.GetInt64(++index),
                            ExcludedLayerIds = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            TargetPremium = reader.GetDecimal(++index),
                            Override = reader.GetDecimal(++index),
                            ManagementFee = reader.GetDecimal(++index),
                            ProfitComm = reader.GetDecimal(++index),
                            PerformanceFee = reader.GetDecimal(++index),
                            RHOE = reader.GetDecimal(++index),
                            HurdleRate = reader.GetDecimal(++index),
                            IsPortIn = reader.GetBoolean(++index),
                            IsPortOut = reader.GetBoolean(++index),
                            RetroBufferType = reader.GetInt32(++index),
                            CessionCapBufferPct = reader.GetDecimal(++index),
                            RetroValuesToBuffer = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            ExcludedContractType = reader.GetInt32(++index),
                        };

                        retroInvestors.Add(retroInvestor);
                    }
                }

                return retroInvestors;
            });
        }

        public Task<IEnumerable<RetroZone>> GetRetroZones()
        {
            return Task.Factory.StartNew(() =>
            {
                return _repository.ExecuteReaderSql(Translate(GET_RETRO_ZONE)).GetObjects<RetroZone>();
            });
        }

        public Task<IDictionary<int, RetroProfile>> GetRetroProfiles()
        {
            return Task.Factory.StartNew(() =>
            {
                IDictionary<int, RetroProfile> retroProfiles = new Dictionary<int, RetroProfile>();
                using (var reader = _repository.ExecuteReaderSql(Translate(GET_RETRO_PROFILES)))
                {
                    while (reader.Read())
                    {
                        int index = 0;
                        RetroProfile retroProfile = new()
                        {
                            RetroProfileId = reader.GetInt32(index),
                            Name = reader.GetString(++index),
                            RegisId = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            ManagerId = reader.GetInt32(++index),
                            CompanyId = reader.GetInt32(++index),
                            OfficeId = reader.GetInt32(++index),
                            DeptId = reader.GetInt32(++index),
                            //CreateDate = reader.GetDateTime(++index),
                            //CreateUser = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //ModifyDate = reader.GetDateTime(++index),
                            //ModifyUser = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //IsActive = reader.GetBoolean(++index),
                            //IsDeleted = reader.GetBoolean(++index),
                            RowVersion = reader.GetInt64(++index),
                        };

                        retroProfiles[retroProfile.RetroProfileId] = retroProfile;
                    }
                }

                return retroProfiles;
            });
        }

        #endregion Retro Info

        #region Retro Layers

        public Task<IEnumerable<RetroLayer>> GetRetroLayers()
        {
            return Task.Factory.StartNew(() =>
            {
                return _repository.ExecuteReaderSql(Translate(GET_RETRO_LAYERS)).GetObjects<RetroLayer>();
            });
        }

        public Task<IEnumerable<RetroLayer>> GetRetroLayers(long afterRowVersion)
        {
            return Task.Factory.StartNew(() =>
            {
                return _repository.ExecuteReaderSql(Translate(string.Format(GET_RETRO_LAYERS_INCREMENTAL, afterRowVersion))).GetObjects<RetroLayer>();
            });
        }

        #endregion Retro Layers

        #region Portfolio Retro Layers

        public Task<IEnumerable<PortfolioRetroLayer>> GetPortfolioRetroLayers()
        {
            return Task.Factory.StartNew(() =>
            {
                return _repository.ExecuteReaderSql(Translate(GET_PORTFOLIO_RETRO_LAYERS)).GetObjects<PortfolioRetroLayer>();
            });
        }

        public Task<IEnumerable<PortfolioRetroLayer>> GetPortfolioRetroLayers(long afterRowVersion)
        {
            return Task.Factory.StartNew(() =>
            {
                return _repository.ExecuteReaderSql(Translate(string.Format(GET_PORTFOLIO_RETRO_LAYERS_INCREMENTAL, afterRowVersion))).GetObjects<PortfolioRetroLayer>();
            });
        }

        #endregion Portfolio Retro Layers

        #region Portfolio Info

        public Task<Dictionary<int, Portfolio>> GetPortfolios()
        {
            return Task.Factory.StartNew(() =>
            {
                return _repository.ExecuteReaderSql(GET_PORTFOLIOS).GetObjects<Portfolio>().ToDictionary(x => x.PortfolioId);
            });
        }

        public Task<Dictionary<int, PortLayer>> GetPortfolioLayers()
        {
            return Task.Factory.StartNew(() =>
            {
                return _repository.ExecuteReaderSql(GET_PORTFOLIO_LAYERS).GetObjects<PortLayer>().ToDictionary(x => x.PortLayerId);
            });
        }

        #endregion Portfolio Info

        #region Retro Cession Info

        public Task<IList<RetroAllocation>> GetRetroAllocations()
        {
            return Task.Factory.StartNew(() =>
            {
                IList<RetroAllocation> retroAllocations = new List<RetroAllocation>();
                using (var reader = _repository.ExecuteReaderSql(Translate(GET_RETRO_ALLOCATION)))
                {
                    while (reader.Read())
                    {
                        int index = 0;
                        RetroAllocation retroAllocation = new()
                        {
                            RetroAllocationId = reader.GetInt32(index),
                            //ROL = reader.GetDecimal(++index),
                            //EL = reader.GetDecimal(++index),
                            //Zone = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //Message = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            LayerId = reader.GetInt32(++index),
                            RetroInvestorId = reader.GetInt32(++index),
                            //CreateDate = reader.GetDateTime(++index),
                            //CreateUser = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //ModifyDate = reader.GetDateTime(++index),
                            //ModifyUser = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //IsActive = reader.GetBoolean(++index),
                            //IsDeleted = reader.GetBoolean(++index),
                            //RegisStatus = reader.GetInt32(++index),
                            //RegisMessage = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            CessionNet = reader.GetDecimal(++index),
                            //CessionDemand = reader.GetDecimal(++index),
                            CessionGross = reader.GetDecimal(++index),
                            RowVersion = reader.GetInt64(++index),
                            CessionCapFactor = reader.GetDecimal(++index),
                            //CessionCapFactorSent = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //CessionGrossFinalSent = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //CessionNetFinalSent = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //AllocationStatus = reader.GetInt32(++index),
                            Override = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            Brokerage = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            Taxes = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //OverrideSent = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //BrokerageSent = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //TaxesSent = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            ManagementFee = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            TailFee = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //IsPortInExpiredLayer = reader.GetBoolean(++index),
                            TopUpZoneId = reader.IsDBNull(++index) ? null : reader.GetInt32(index),
                            CessionPlaced = reader.GetDecimal(++index),
                        };

                        retroAllocations.Add(retroAllocation);
                    }
                }

                return retroAllocations;
            });
        }

        public Task<IEnumerable<PortLayerCession>> GetPortfolioLayerCessions()
        {
            Console.Write("0 - Non Parallel ");
            return Task.Factory.StartNew<IEnumerable<PortLayerCession>>(() =>
            {
                return _repository.ExecuteReaderSql(GET_PORTFOLIO_LAYER_CESSIONS).GetObjects<PortLayerCession>();
            });
        }

        public Task<IEnumerable<PortLayerCession>> GetPortfolioLayerCessionsParallel(int partitionCount = 8)
        {
            return Task.Factory.StartNew(() =>
            {
                PortLayerCession[][] portLayerCessions = new PortLayerCession[partitionCount][];
                Task[] portLayerCessionsTasks = new Task[partitionCount];
                for (int i = 0; i < portLayerCessionsTasks.Length; i++)
                    portLayerCessionsTasks[i] = Task.Factory.StartNew(state =>
                    {
                        int index = (int)state!;
                        portLayerCessions[index] = _repository.ExecuteReaderSql(string.Format(GET_PORTFOLIO_LAYER_CESSIONS_BY_PARTITION, index, partitionCount)).GetObjects<PortLayerCession>().ToArray();
                    }, i);
                Task.WaitAll(portLayerCessionsTasks);
                return portLayerCessions.SelectMany(x => x);
            });
        }

        public Task<PortfolioRetroCessions> GetPortfolioRetroCessionView(ResetType resetType = ResetType.LOD, int partitionCount = 8)
        {
            return Task.Factory.StartNew(() =>
            {
                var portLayersTask = GetPortfolioLayers();
                var layersTask = GetLayers();
                var portfoliosTask = GetPortfolios();
                var retroProgramsTask = GetRetroPrograms();
                Task.WaitAll(portLayersTask, layersTask, portfoliosTask, retroProgramsTask);
                Dictionary<int, PortLayer> portLayers = portLayersTask.Result;
                Dictionary<int, Layer> layers = layersTask.Result;
                Dictionary<int, Portfolio> portfolios = portfoliosTask.Result;
                Dictionary<int, RetroProgram> retroPrograms = retroProgramsTask.Result;
                List<PortLayerCessionExtended>[] partitionedPortLayerCessions = new List<PortLayerCessionExtended>[partitionCount];

                for (int i = 0; i < partitionedPortLayerCessions.Length; i++)
                    partitionedPortLayerCessions[i] = new();

                Task[] portLayerCessionsTasks = new Task[partitionCount];
                for (int i = 0; i < portLayerCessionsTasks.Length; i++)
                    portLayerCessionsTasks[i] = Task.Factory.StartNew(state =>
                    {
                        var input = ((int index, List<PortLayerCessionExtended> layerCessionRepo))state!;

                        foreach (var portLayerCession in _repository.ExecuteReaderSql(string.Format(GET_PORTFOLIO_LAYER_CESSIONS_BY_PARTITION, input.index, partitionCount)).GetObjects<PortLayerCessionExtended>())
                        {
                            if (!retroPrograms.TryGetValue(portLayerCession.RetroProgramId, out RetroProgram retroProgram))
                                continue;

                            PortLayer portLayer = portLayers[portLayerCession.PortLayerId];
                            Portfolio portfolio = portfolios[portLayer.PortfolioId];
                            Layer layer = layers[portLayer.LayerId];
                            portLayerCession.PortfolioId = portfolio.PortfolioId;
                            portLayerCession.LayerId = layer.LayerId;
                            portLayerCession.RetroLevelType = retroProgram.RetroLevelType;
                            DateTime? inception;
                            if ((inception = GetPortfolioLayerInception(portfolio, layer)) == null)
                                continue;
                            DateTime portLayerInception = (DateTime)inception;
                            DateTime portLayerExpiration = portLayerInception.AddYears(1).AddDays(-1);

                            if (portLayerExpiration < retroProgram.Inception
                                || portLayerInception > retroProgram.Expiration
                                || (retroProgram.RetroProgramType != RetroProgramType.LOD /*1 = LOD*/ && portLayerInception < retroProgram.Inception))/*if RAD, discard ones where the layer started before the retro*/
                                continue;

                            portLayerCession.OverlapStart = retroProgram.RetroProgramType != RetroProgramType.RAD /*2 = RAD*/ && retroProgram.Inception > portLayerInception
                                ? retroProgram.Inception
                                : portLayerInception;
                            portLayerCession.OverlapEnd = retroProgram.RetroProgramType != RetroProgramType.RAD /*2 = RAD*/ && retroProgram.Expiration < portLayerExpiration
                                ? retroProgram.Expiration
                                : portLayerExpiration;

                            input.layerCessionRepo.Add(portLayerCession);
                        }
                    }, (i, partitionedPortLayerCessions[i]));
                Task.WaitAll(portLayerCessionsTasks);

                return new PortfolioRetroCessions(partitionedPortLayerCessions.SelectMany(cession => cession));

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                static DateTime? GetPortfolioLayerInception(Portfolio portfolio, Layer layer)
                {
                    return portfolio.PortfolioType switch
                    {
                        0 => layer.Inception,
                        1 => layer.Inception.AddYears(1),
                        _ when portfolio.PortfolioType == 2 && layer.Inception.Year == portfolio.AsOfDate.Year => layer.Inception,
                        _ when portfolio.PortfolioType == 2 && layer.Inception.Year == portfolio.AsOfDate.Year - 1 => layer.Inception.AddYears(1),
                        _ when portfolio.PortfolioType == 3 && layer.Inception.Year == portfolio.AsOfDate.Year => layer.Inception.AddYears(1),
                        _ when portfolio.PortfolioType == 3 && layer.Inception.Year == portfolio.AsOfDate.Year - 1 => layer.Inception.AddYears(2),
                        _ => null
                    };
                }
            });
        }

        public Task<RetroCessions> GetRetroCessionView(ResetType resetType = ResetType.LOD)
        {
            return Task.Factory.StartNew(() =>
            {
                var retroAllocationsTask = GetRetroAllocations();
                var retroInvestorsTask = GetRetroInvestors();
                var spInsurersTask = GetSPInsurers();
                var layerRetroPlacementsTask = GetLayerRetroPlacements();
                var investorResetCessionsTask = GetInvestorResetCessions();
                var investorInitialCessionsTask = GetInvestorInitialCessions();
                var layersTask = GetLayers();
                var retroProgramsTask = GetRetroPrograms();

                Task.WaitAll(investorResetCessionsTask, investorInitialCessionsTask);
                InvestorCession[] investorInitialCessions = investorInitialCessionsTask.Result.ToArray();
                InvestorCession[] investorResetCessions = investorResetCessionsTask.Result.ToArray();
                Dictionary<(int RetroProgramId, int RetroInvestorId), InvestorCession[]> investorRetroCessionPeriods = investorResetCessions.Union(investorInitialCessions.Except(investorResetCessions, new InvestorRetroProgramResetDateComparer()))
                    .GroupBy(g => (g.RetroProgramId, g.RetroInvestorId))
                    .ToDictionary(k => k.Key, v => v.OrderBy(o => o.StartDate).ToArray());//take investor cessions preferably from the RetroInvestorReset table rather than the RetroInvestor table. 

                Task.WaitAll(retroAllocationsTask, retroInvestorsTask, spInsurersTask, layerRetroPlacementsTask);
                IList<RetroAllocation> retroAllocations = retroAllocationsTask.Result;
                IList<RetroInvestor> retroInvestors = retroInvestorsTask.Result;
                Dictionary<int, SPInsurer> spInsurers = spInsurersTask.Result;
                Dictionary<int, Layer> layers = layersTask.Result;
                Dictionary<(int LayerId, int RetroProgramId), LayerRetroPlacement> layerRetroPlacements = layerRetroPlacementsTask.Result
                    .ToDictionary(x => (x.LayerId, x.RetroProgramId));

                var retroInvestorPrograms = retroInvestors
                    .Join(spInsurers, ok => ok.SPInsurerId, ik => ik.Value.SPInsurerId, (o, i) => new { o, i.Value.RetroProgramId });
                Dictionary<(int RetroProgramId, int LayerId), (decimal GrossCessionBeforePlacement, decimal CalculatedGrossCessionBeforePlacement, decimal Placement, decimal GrossCessionAfterPlacement, int[] RetroInvestors)> retroProgramsLayerGrossAllocation =
                    retroAllocations
                    .Join(retroInvestorPrograms, ok => ok.RetroInvestorId, ik => ik.o.RetroInvestorId, (o, i) => new { o, i.RetroProgramId })
                    .Join(layers, ok => ok.o.LayerId, ik => ik.Key, (o, i) => new { o, LayerInception = i.Value.Inception })
                    .LeftOuterJoin(layerRetroPlacements, ok => (ok.o.o.LayerId, ok.o.RetroProgramId), ik => ik.Key, (o, i) => new { o, Placement = i.Value?.Placement ?? 1.0M })
                    .LeftOuterJoin(investorRetroCessionPeriods, ok => (ok.o.o.RetroProgramId, ok.o.o.o.RetroInvestorId), ik => ik.Key, (o, i) => new { o, CessionBeforePlacement = i.Value.LastOrDefault(ii => ii.StartDate <= o.o.LayerInception)?.CessionBeforePlacement ?? i.Value[0].CessionBeforePlacement, i.Key.RetroInvestorId }) //to handle the case of RetroProgram 101 wih Retro Zone Placement = 0 but non zero layer retro cessions
                    .GroupBy(g => (g.o.o.o.RetroProgramId, g.o.o.o.o.LayerId))
                    .ToDictionary(k => k.Key
                        , v => (v.Sum(x => x.CessionBeforePlacement)
                        , v.Sum(x => x.o.Placement == decimal.Zero ? x.CessionBeforePlacement : x.o.o.o.o.CessionGross / x.o.Placement)
                        , v.Max(x => x.o.Placement), v.Sum(x => x.o.o.o.o.CessionGross)
                        , v.Select(x => x.RetroInvestorId).ToArray()));

                Dictionary<int, (DateTime StartDate, int RetroProgramResetId)[]> retroProgramResetDates = investorRetroCessionPeriods
                    .GroupBy(x => x.Key.RetroProgramId, y => y.Value.Select(z => (z.StartDate, z.RetroProgramResetId)))
                    .ToDictionary(k => k.Key, v => v.SelectMany(s => s).Distinct().OrderBy(vv => vv.StartDate).ToArray());
                Dictionary<int, RetroProgram> retroPrograms = retroProgramsTask.Result;
                List<RetroLayerCession> retroLayerCessions = new();
                /*TODO: Note that there are cases where the CalculatedGrossCessionBeforePlacement is different from GrossCessionBeforePlacement. In all the cases I could check, the CalculatedGrossCessionBeforePlacement was the correct one but keep in mind this mismatch*/
                foreach (var retroGrossAllocation in retroProgramsLayerGrossAllocation.OrderByDescending(x => x.Key.RetroProgramId))
                {
                    int retroProgramId = retroGrossAllocation.Key.RetroProgramId;
                    (DateTime StartDate, int RetroProgramResetId)[] resetDates = retroProgramResetDates[retroProgramId];
                    RetroProgram retroProgram = retroPrograms[retroGrossAllocation.Key.RetroProgramId];

                    if (!layers.TryGetValue(retroGrossAllocation.Key.LayerId, out Layer layer)
                        || !TryGetLayerRetroIntersection(layer, retroProgram, out DateTime overlapStart, out DateTime overlapEnd))
                        continue;

                    if (resetType == ResetType.LOD)
                    {                        
                        if (resetDates.Length == 1)
                        {
                            var initialCession = resetDates[0];
                            retroLayerCessions.Add(new RetroLayerCession
                            {
                                RetroProgramId = retroProgramId,
                                LayerId = retroGrossAllocation.Key.LayerId,
                                RetroProgramResetId = initialCession.RetroProgramResetId,
                                CessionGross = retroGrossAllocation.Value.GrossCessionAfterPlacement,
                                RetroLevelType = retroProgram.RetroLevelType,
                                OverlapStart = overlapStart,
                                OverlapEnd = overlapEnd,
                                ResetType = ResetType.LOD
                            });
                        }
                        else
                        {
                            int[] retroInvestorIds = retroGrossAllocation.Value.RetroInvestors;
                            InvestorCession[][] investorCessions = new InvestorCession[retroInvestorIds.Length][];
                            for (int j = 0; j < retroInvestorIds.Length; ++j)
                                investorCessions[j] = investorRetroCessionPeriods[(retroProgramId, retroInvestorIds[j])];

                            Dictionary<DateTime, decimal> resetDateGrossCessionAfterPlacement = investorCessions
                                .SelectMany(x => x)
                                .GroupBy(g => g.StartDate)
                                .ToDictionary(k => k.Key, v => v.Sum(vv => vv.CessionBeforePlacement) * (retroGrossAllocation.Value.Placement == decimal.Zero ? retroGrossAllocation.Value.GrossCessionAfterPlacement / retroGrossAllocation.Value.CalculatedGrossCessionBeforePlacement : retroGrossAllocation.Value.Placement));
                            for (int i = 0; i < resetDates.Length; i++)
                            {
                                var resetCession = resetDates[i];
                                DateTime resetStart = resetDates[i].StartDate;
                                if (i == 0 && resetStart != retroProgram.Inception)
                                    throw new InvalidDataException("Expected the initial cession date to match the retro inception date");
                                DateTime resetEnd = i + 1 >= resetDates.Length ? retroProgram.Expiration : resetDates[i + 1].StartDate.AddDays(-1);
                                if (!TryGetPeriodIntersection(resetStart, resetEnd, retroProgram.Inception, retroProgram.Expiration, out DateTime retroOverlapStart, out DateTime retroOverlapEnd)
                                 || !TryGetLayerRetroIntersection(layer, retroProgram.RetroProgramType, retroOverlapStart, retroOverlapEnd, out DateTime resetOverlapStart, out DateTime resetOverlapEnd))
                                    continue;
                                retroLayerCessions.Add(new RetroLayerCession
                                {
                                    RetroProgramId = retroProgramId,
                                    LayerId = retroGrossAllocation.Key.LayerId,
                                    RetroProgramResetId = resetCession.RetroProgramResetId,
                                    CessionGross = resetDateGrossCessionAfterPlacement[resetStart],
                                    RetroLevelType = retroProgram.RetroLevelType,
                                    OverlapStart = resetOverlapStart,
                                    OverlapEnd = resetOverlapEnd,
                                    ResetType = ResetType.LOD
                                });
                            }
                        }
                    }
                    else if (resetType == ResetType.RAD)
                    {
                        if (resetDates.Length == 1)
                        {
                            var initialCession = resetDates[0];
                            if (initialCession.RetroProgramResetId != InvestorCession.DefaultRetroProgramResetId)
                                throw new Exception($"Unexpected Retro Program Reset Id {initialCession.RetroProgramResetId}");
                            retroLayerCessions.Add(new RetroLayerCession
                            {
                                RetroProgramId = retroProgramId,
                                LayerId = retroGrossAllocation.Key.LayerId,
                                RetroProgramResetId = initialCession.RetroProgramResetId,
                                CessionGross = retroGrossAllocation.Value.GrossCessionAfterPlacement,
                                RetroLevelType = retroProgram.RetroLevelType,
                                OverlapStart = overlapStart,
                                OverlapEnd = overlapEnd,
                                ResetType = ResetType.RAD
                            });
                        }
                        else
                        {
                            int[] retroInvestorIds = retroGrossAllocation.Value.RetroInvestors;
                            InvestorCession[][] investorCessions = new InvestorCession[retroInvestorIds.Length][];
                            for (int j = 0; j < retroInvestorIds.Length; ++j)
                                investorCessions[j] = investorRetroCessionPeriods[(retroProgramId, retroInvestorIds[j])];

                            Dictionary<DateTime, decimal> resetDateGrossCessionAfterPlacement = investorCessions
                                .SelectMany(x => x)
                                .GroupBy(g => g.StartDate)
                                .ToDictionary(k => k.Key, v => v.Sum(vv => vv.CessionBeforePlacement) * (retroGrossAllocation.Value.Placement == decimal.Zero ? retroGrossAllocation.Value.GrossCessionAfterPlacement / retroGrossAllocation.Value.CalculatedGrossCessionBeforePlacement : retroGrossAllocation.Value.Placement));

                            (DateTime StartDate, int RetroProgramResetId) resetCession = default;
                            if (retroProgram.RetroProgramType == RetroProgramType.LOD && resetDates[0].StartDate > layer.Inception)
                                resetCession = resetDates[0];
                            else
                                resetCession = resetDates.Last(x => x.StartDate <= layer.Inception);

                            DateTime resetStart = resetCession.StartDate;                            
                            DateTime resetEnd = retroProgram.Expiration;
                            if (!TryGetPeriodIntersection(resetStart, resetEnd, retroProgram.Inception, retroProgram.Expiration, out DateTime retroOverlapStart, out DateTime retroOverlapEnd)
                                || !TryGetLayerRetroIntersection(layer, retroProgram.RetroProgramType, retroOverlapStart, retroOverlapEnd, out DateTime resetOverlapStart, out DateTime resetOverlapEnd))
                                continue;
                            retroLayerCessions.Add(new RetroLayerCession
                            {
                                RetroProgramId = retroProgramId,
                                LayerId = retroGrossAllocation.Key.LayerId,
                                RetroProgramResetId = resetCession.RetroProgramResetId,
                                CessionGross = resetDateGrossCessionAfterPlacement[resetStart],
                                RetroLevelType = retroProgram.RetroLevelType,
                                OverlapStart = resetOverlapStart,
                                OverlapEnd = resetOverlapEnd,
                                ResetType = ResetType.RAD
                            });
                            
                        }
                    }
                    else throw new NotImplementedException($"RssetType {resetType} not implemented.");
                }

                return new RetroCessions(retroLayerCessions);
            });
        }

        public Task<IEnumerable<RetroCession>> GetRetroResetCessions()
        {
            return Task.Factory.StartNew(() =>
            {
                var retroInvestorsResetTask = GetRetroInvestorResets();
                var retroProgramResetTask = GetRetroProgramResets();
                Task.WaitAll(retroInvestorsResetTask, retroProgramResetTask);
                var retroInvestorsResets = retroInvestorsResetTask.Result;
                var retroProgramResets = retroProgramResetTask.Result;
                return retroInvestorsResets
                    .Join(retroProgramResets, ok => ok.RetroProgramResetId, ik => ik.RetroProgramResetId, (o, i) => new { i.RetroProgramId, i.StartDate, i.RetroProgramResetId, o })
                    .GroupBy(g => (g.RetroProgramId, g.StartDate, g.RetroProgramResetId))
                    .Select(x => new RetroCession(x.Key.RetroProgramResetId, x.Key.RetroProgramId, x.Key.StartDate, x.Sum(oo => oo.o.InvestmentSignedAmt), x.Max(oo => oo.o.TargetCollateral), x.Sum(oo => oo.o.InvestmentSigned)))
                    //.Where(r => r.CessionBeforePlacement != 0)
                    ;
            });
        }

        public Task<IEnumerable<RetroCession>> GetRetroInitialCessions()
        {
            return Task.Factory.StartNew(() =>
            {
                var retroInvestorsTask = GetRetroInvestors();
                var spInsurersTask = GetSPInsurers();
                var retroProgramTask = GetRetroPrograms();
                Task.WaitAll(retroInvestorsTask, spInsurersTask, retroProgramTask);
                var retroInvestors = retroInvestorsTask.Result;
                var spInsurers = spInsurersTask.Result;
                var retroPrograms = retroProgramTask.Result;
                return retroInvestors
                    .Join(spInsurers, ri => ri.SPInsurerId, spi => spi.Key, (ri, spi) => new { spi.Value.RetroProgramId, ri })
                    .GroupBy(temp => temp.RetroProgramId)
                    .Join(retroPrograms, ok => ok.Key, ik => ik.Key, (o, i) => new RetroCession(-1, i.Value.RetroProgramId, i.Value.Inception, o.Sum(oo => oo.ri.InvestmentSignedAmt), o.Max(oo => oo.ri.TargetCollateral), o.Sum(oo => oo.ri.InvestmentSigned)))
                    //.Where(r => r.CessionBeforePlacement != 0)
                    ;
            });
        }

        public Task<IEnumerable<InvestorCession>> GetInvestorResetCessions()
        {
            return Task.Factory.StartNew(() =>
            {
                var retroInvestorsResetTask = GetRetroInvestorResets();
                var retroProgramResetTask = GetRetroProgramResets();
                Task.WaitAll(retroInvestorsResetTask, retroProgramResetTask);
                var retroInvestorsResets = retroInvestorsResetTask.Result;
                var retroProgramResets = retroProgramResetTask.Result;
                return retroInvestorsResets
                    .Join(retroProgramResets, ok => ok.RetroProgramResetId, ik => ik.RetroProgramResetId, (o, i) => new { i.RetroProgramId, i.StartDate, i.RetroProgramResetId, o })
                    .GroupBy(g => (g.RetroProgramId, g.StartDate, g.RetroProgramResetId, g.o.RetroInvestorId))
                    .Select(x => new InvestorCession(x.Key.RetroInvestorId, x.Key.RetroProgramResetId, x.Key.RetroProgramId, x.Key.StartDate, x.Sum(oo => oo.o.InvestmentSignedAmt), x.Max(oo => oo.o.TargetCollateral), x.Sum(oo => oo.o.InvestmentSigned)))
                    //.Where(r => r.CessionBeforePlacement != 0)
                    ;
            });
        }

        public Task<IEnumerable<InvestorCession>> GetInvestorInitialCessions()
        {
            return Task.Factory.StartNew(() =>
            {
                var retroInvestorsTask = GetRetroInvestors();
                var spInsurersTask = GetSPInsurers();
                var retroProgramTask = GetRetroPrograms();
                Task.WaitAll(retroInvestorsTask, spInsurersTask, retroProgramTask);
                var retroInvestors = retroInvestorsTask.Result;
                var spInsurers = spInsurersTask.Result;
                var retroPrograms = retroProgramTask.Result;
                return retroInvestors
                    .Join(spInsurers, ri => ri.SPInsurerId, spi => spi.Key, (ri, spi) => new { spi.Value.RetroProgramId, ri })
                    .GroupBy(temp => (temp.RetroProgramId, temp.ri.RetroInvestorId))
                    .Join(retroPrograms, ok => ok.Key.RetroProgramId, ik => ik.Key, (o, i) => new InvestorCession(o.Key.RetroInvestorId, InvestorCession.DefaultRetroProgramResetId, i.Value.RetroProgramId, i.Value.Inception, o.Sum(oo => oo.ri.InvestmentSignedAmt), o.Max(oo => oo.ri.TargetCollateral), o.Sum(oo => oo.ri.InvestmentSigned)))
                    //.Where(r => r.CessionBeforePlacement != 0)
                    ;
            });
        }

        public Task<IEnumerable<LayerRetroPlacement>> GetLayerRetroPlacements()
        {
            return Task.Factory.StartNew(() =>
            {
                var layerTopUpZonesTask = GetLayerTopUpZones();
                var retroZonesTask = GetRetroZones();
                Task.WaitAll(layerTopUpZonesTask, retroZonesTask);
                IEnumerable<LayerTopUpZone> layerTopUpZones = layerTopUpZonesTask.Result;
                RetroZone[] retroZones = retroZonesTask.Result.ToArray();
                var retroZonePlacements = retroZones.GroupBy(g => (g.RetroProgramId, g.TopUpZoneId))
                    .Select(x => new { x.Key, MaxCession = x.Max(xx => xx.Cession), MinCession = x.Min(xx => xx.Cession) });

                if (retroZonePlacements.Any(x => x.MaxCession != x.MinCession))
                    throw new NotImplementedException("Different Placements for the same zone and retro program at different dates not handled,");

                return layerTopUpZones
                    .Join(retroZonePlacements, ok => ok.TopUpZoneId, ik => ik.Key.TopUpZoneId, (o, i) => new LayerRetroPlacement(o.LayerId, i.Key.RetroProgramId, i.MaxCession))
                    ;
            });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Range[] GetYeltDayRanges(in DateTime inceptionDate, in DateTime expirationDate)
        {
            const int firstDayOfYear = 1;
            const int lastDayOfYear = 365;
            int days = (expirationDate - inceptionDate).Days + 1;
            if (days < 0)
                throw new ArgumentException("Expected an Expiration Date >= Inception Date");
            if (days >= lastDayOfYear)
                return [new Range(firstDayOfYear, lastDayOfYear)];
            if (inceptionDate.DayOfYear > expirationDate.DayOfYear)  //period intersecting two successive calendar years
                return [new Range(firstDayOfYear, expirationDate.DayOfYear), new Range(inceptionDate.DayOfYear, lastDayOfYear)];
            else return [new Range(inceptionDate.DayOfYear, expirationDate.DayOfYear)];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryGetPeriodIntersection(in DateTime inceptionA, in DateTime expirationA, in DateTime inceptionB, in DateTime expirationB, out DateTime overlapStart, out DateTime overlapEnd)
        {
            overlapStart = inceptionA > inceptionB ? inceptionA : inceptionB;
            overlapEnd = expirationA > expirationB ? expirationB : expirationA;
            return overlapStart <= overlapEnd;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryGetLayerRetroIntersection(in Layer layer, in RetroProgram retroProgram, out DateTime overlapStart, out DateTime overlapEnd)
            => TryGetLayerRetroIntersection(layer, retroProgram.RetroProgramType, retroProgram.Inception, retroProgram.Expiration, out overlapStart, out overlapEnd);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryGetLayerRetroIntersection(in Layer layer, in RetroProgramType retroProgramType, in DateTime retroInception, in DateTime retroExpiration, out DateTime overlapStart, out DateTime overlapEnd)
        {
            if (retroProgramType == RetroProgramType.RAD)
            {
                if (layer.Inception < retroInception || layer.Inception > retroExpiration)
                {
                    overlapStart = DateTime.MinValue;
                    overlapEnd = DateTime.MinValue;
                    return false;
                }
                else
                {
                    overlapStart = layer.Inception;
                    overlapEnd = layer.Expiration;
                    return true;
                }
            }
            else if (retroProgramType == RetroProgramType.LOD)
            {
                overlapStart = layer.Inception > retroInception ? layer.Inception : retroInception;
                overlapEnd = layer.Expiration > retroExpiration ? retroExpiration : layer.Expiration;
                return overlapStart <= overlapEnd;
            }
            else throw new NotImplementedException(retroProgramType.ToString());
        }



        #endregion Retro Cession Info

        #region FX Rates

        public Task<FXTable> GetFXRates()
        {
            return Task.Factory.StartNew(() =>
            {
                FXTable fxTable = new FXTable();
                using (var reader = _repository.ExecuteReaderSql(Translate(GET_FX_RATES)))
                {
                    while (reader.Read())
                    {
                        int index = 0;
                        FXRate fxRate = new()
                        {
                            FXRateId = reader.GetInt32(index),
                            BaseCurrency = reader.GetString(++index),
                            Currency = reader.GetString(++index),
                            FXDate = reader.GetDateTime(++index),
                            Rate = reader.GetDecimal(++index),
                            //CreateDate = reader.GetDateTime(++index),
                            //CreateUser = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //ModifyDate = reader.GetDateTime(++index),
                            //ModifyUser = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //IsActive = reader.GetBoolean(++index),
                            //IsDeleted = reader.GetBoolean(++index),
                            RowVersion = reader.GetInt64(++index),
                        };

                        fxTable.AddRate(fxRate);
                    }
                }

                return fxTable;
            });
        }

        #endregion FX Rates

        #region Layer Loss Analyses

        public Task<IEnumerable<LayerLossAnalysis>> GetLayerLossAnalyses()
        {
            return Task.Factory.StartNew(() =>
            {
                return _repository.ExecuteReaderSql(Translate(GET_LAYER_LOSS_ANALYSES)).GetObjects<LayerLossAnalysis>();
            });
        }

        public Task<IEnumerable<LayerLossAnalysis>> GetLayerLossAnalyses(long afterRowVersion)
        {
            return Task.Factory.StartNew(() =>
            {
                return _repository.ExecuteReaderSql(Translate(string.Format(GET_LAYER_LOSS_ANALYSES_INCREMENTAL, afterRowVersion))).GetObjects<LayerLossAnalysis>();
            });
        }

        #endregion Layer Loss Analyses

        #region Query Conversion

        protected virtual string Translate(in string sqlQuery) => sqlQuery;

        #endregion Query Conversion

        #endregion Methods

        #region Types

        private class InvestorRetroProgramResetDateComparer : IEqualityComparer<InvestorCession>
        {
            public bool Equals([DisallowNull] InvestorCession x, [DisallowNull] InvestorCession y)
            {
                return x.RetroInvestorId == y.RetroInvestorId
                    && x.RetroProgramId == y.RetroProgramId
                    && x.StartDate == y.StartDate;
            }

            public int GetHashCode([DisallowNull] InvestorCession obj)
            {
                return obj.RetroInvestorId ^ obj.RetroProgramId ^ obj.StartDate.GetHashCode();
            }
        }

        #endregion Types

        #region Constants

        private const string GET_LAYERS = @"SELECT LayerId
     , Inception
     , Expiration
  FROM dbo.Layer
 WHERE IsActive = 1
   AND IsDeleted = 0";

        private const string GET_LAYER_DETAILS = @"SELECT LayerId
      ,SubmissionId
      --,LayerNum
      --,SubLayerNum
      --,ReinstCount
      ,Placement
      ,OccLimit
      --,OccRetention
      --,CascadeRetention
      --,AAD
      --,Var1Retention
      --,Var2Retention
      ,AggLimit
      --,AggRetention
      --,Franchise
      --,FranchiseReverse
      ,RiskLimit
      ,Inception
      ,UWYear
      ,Expiration
      --,ExpirationFinal
      --,Facility
      --,Segment
      --,LOB
      --,ContractType
      ,LimitBasis
      --,AttachBasis
      --,LAETerm
      --,LossTrigger
      --,ROL
      --,QuoteROL
      --,ERC
      --,ERCModel
      --,ERCMid
      --,ERCPareto
      --,RegisId
      --,RegisNbr
      --,RegisMKey
      --,RegisIdCt
      --,RegisNbrCt
      --,BurnReported
      --,BurnTrended
      --,YearPeriodSelected
      --,YearPeriodLoss
      --,CatLoss1
      --,CatLoss2
      --,CatLoss3
      ,EstimatedShare
      ,SignedShare
      --,AuthShare
      --,QuotedShare
      ,CONVERT(TINYINT, Status) AS Status
      --,LayerDesc
      --,Notes
      --,RegisMsg
      --,ExpiringLayerId
      --,Commission
      --,CommOverride
      --,Brokerage
      --,Tax
      --,OtherExpenses
      --,IsVarComm
      --,VarCommHi
      --,VarCommLow
      --,IsGrossUpComm
      --,GrossUpFactor
      --,IsSlidingScale
      --,SSCommProv
      --,SSCommMax
      --,SSCommMin
      --,SSLossRatioProv
      --,SSLossRatioMax
      --,SSLossRatioMin
      --,IsProfitComm
      --,ProfitComm
      --,CCFYears
      --,DCFYears
      --,DCFAmount
      --,PCStartDate
      --,ComAccountProtect
      --,ProfitCommissionExpAllowance
      --,Rate
      --,PremiumFreq
      --,AdjustmentBaseType
      --,LayerType
      --,FHCFBand
      --,CreateDate
      --,CreateUser
      --,ModifyDate
      --,ModifyUser
      --,IsActive
      --,IsDeleted
      --,TopUpZoneId
      --,ERCQuote
      --,DeclineReason
      --,InuringLimit
      --,RiskRetention
      --,ReinsurerExpenses
      --,LayerCategory
      --,LayerCatalog
      ,Premium
      --,QuotePremium
      --,RiskZoneId
      --,RelShare
      --,TargetNetShare
      --,RegisLayerCode
      --,SnpLobId
      --,InvestmentReturn
      --,NonCatMarginAllowance
      --,LossDuration
      --,DiversificationFactor
      --,EarningType
      --,SourceId
      --,OrderPct
      --,BrokerRef
      --,AcctBrokerId
      --,IsAdditionalPremium
      --,IsCommonAcct
      --,EventNumber
      --,IsStopLoss
      --,StopLossLimitPct
      --,StopLossAttachPct
      --,IsLossCorridor
      --,LossCorridorBeginPct
      --,LossCorridorEndPct
      --,LossCorridorCedePct
      --,OccLimitInPct
      --,OccRetnInPct
      --,ExpiringCorreShare
      --,CorreAuthMin
      --,CorreAuthTarget
      --,CorreAuthMax
      --,CorreRenewalMin
      --,SharedToCorre
      --,SignedCorreShare
      --,QuotedCorreShare
      --,AuthCorreShare
      --,FrontingFee
      , CONVERT(BIGINT, RowVersion) AS RowVersion
      --,NonCatWeightPC
      --,NonCatWeightSS
      ,BoundFXRate
      ,BoundFXDate
      --,RegisStatus
      --,IsDifferentialTerms
      --,RolRpp
      --,WILResolution
      --,IsParametric
      --,PricingSource
      --,IRISPolicySeqNumber
      --,IRISStatus
      --,IRISComments
      --,IRISRefId
      --,IRISClassCode
      --,IRISBranchCode
      --,IRISTradeCode
      --,IRISPlacingCode
      --,ExpectedGrossNetPremiumGBP
      --,IRISProductCode
      --,StopLossBufferPct
      --,ERCActual
      --,ERCActualSource
      --,ELMarketShare
      --,ELHistoricalBurn
      --,ELBroker
      --,MAOL
      --,NCBR
      --,IsTerrorismSubLimitAppl
      --,TerrorismSubLimit
      --,TerrorismSubLimitComments
      --,LloydsCapital
      --,LloydsROC
      --,QuoteExpire
      --,AuthExpire
      --,MktROL
      --,IsHidden
      --,Cloud
      --,Ransom
      --,BudgetROL
      --,BudgetPremium
      --,BudgetShare
  FROM dbo.Layer
 WHERE IsActive = 1
   AND IsDeleted = 0";

        private const string GET_LAYER_META_INFOS = @"SELECT LayerId
     , Segment
  FROM dbo.Layer
 WHERE IsActive = 1
   AND IsDeleted = 0";

        private const string GET_LAYER_REINSTATEMENTS = @"SELECT ReinstatementId
     , LayerId
     , [Order]
     , Quantity
     , Premium
     , Brokerage
     , CONVERT(BIGINT, RowVersion) AS RowVersion
  FROM dbo.Reinstatement
 WHERE IsActive = 1
   AND IsDeleted = 0";

        private const string GET_PORTFOLIOS = @"SELECT PortfolioId
     , PortfolioType
     , AsOfDate
  FROM dbo.Portfolio
 WHERE IsActive = 1
   AND IsDeleted = 0";

        private const string GET_PORTFOLIO_LAYERS = @"SELECT PortLayerId
     , LayerId
     , PortfolioId
  FROM dbo.PortLayer
 WHERE IsActive = 1
   AND IsDeleted = 0";

        private const string GET_RETRO_PROGRAM = @"SELECT RetroProgramId
     , RetroProfileId
     , Inception
     , Expiration
     , CONVERT(TINYINT, RetroProgramType) AS RetroProgramType
     , CONVERT(TINYINT, RetroLevelType + 1) AS RetroLevelType
  FROM dbo.RetroProgram
 WHERE /*Status IN (22,10) AND*//*remove projection retros*/
       IsActive = 1
   AND IsDeleted = 0";

        private const string GET_PORTFOLIO_LAYER_CESSIONS = @"SELECT PortLayerCessionId
     , PortLayerId
     , RetroProgramId
     , CessionGross
     /*, CessionNet*/
  FROM dbo.PortLayerCession
 WHERE IsActive = 1
   AND ShouldCessionApply = 1
   AND IsDeleted = 0
   AND CessionGross > 0";

        private const string GET_PORTFOLIO_LAYER_CESSIONS_BY_PARTITION = @"SELECT PortLayerCessionId
     , PortLayerId
     , RetroProgramId
     , CessionGross
     /*, CessionNet*/
  FROM dbo.PortLayerCession
 WHERE (PortLayerCessionId % {1}) = {0} 
   AND IsActive = 1
   AND ShouldCessionApply = 1
   AND IsDeleted = 0
   AND CessionGross > 0";

        private const string GET_RETRO_ALLOCATION = @"SELECT RetroAllocationId
      --,ROL
      --,EL
      --,Zone
      --,Message
      ,LayerId
      ,RetroInvestorId
      --,CreateDate
      --,CreateUser
      --,ModifyDate
      --,ModifyUser
      --,IsActive
      --,IsDeleted
      --,RegisStatus
      --,RegisMessage
      ,CessionNet
      --,CessionDemand
      ,CessionGross
      ,CONVERT(BIGINT, RowVersion) AS RowVersion
      ,CessionCapFactor
      --,CessionCapFactorSent
      --,CessionGrossFinalSent
      --,CessionNetFinalSent
      --,AllocationStatus
      ,Override
      ,Brokerage
      ,Taxes
      --,OverrideSent
      --,BrokerageSent
      --,TaxesSent
      ,ManagementFee
      ,TailFee
      --,IsPortInExpiredLayer
      ,TopUpZoneId
      ,CessionPlaced
  FROM dbo.RetroAllocation
 WHERE IsActive = 1
   AND IsDeleted = 0;";

        private const string GET_RETRO_INVESTOR_RESET = @"SELECT RetroInvestorResetId
      ,RetroInvestorId
      ,RetroProgramResetId
      ,StartDate
      ,TargetCollateral
      ,TargetPremium
      ,InvestmentSignedAmt
      ,InvestmentSigned
      ,CONVERT(BIGINT, RowVersion) AS RowVersion
      --,CreateDate
      --,CreateUser
      --,ModifyDate
      --,ModifyUser
      --,IsActive
      --,IsDeleted
  FROM dbo.RetroInvestorReset
 WHERE IsActive = 1
  AND IsDeleted = 0";

        private const string GET_RETRO_PROGRAM_RESET = @"SELECT RetroProgramResetId
      ,RetroProgramId
      ,StartDate
      ,TargetCollateral
      ,TargetPremium
      ,CONVERT(BIGINT, RowVersion) AS RowVersion
      --,CreateDate
      --,CreateUser
      --,ModifyDate
      --,ModifyUser
      --,IsActive
      --,IsDeleted
  FROM dbo.RetroProgramReset
 WHERE IsActive = 1
   AND IsDeleted = 0";

        private const string GET_SPINSURER = @"SELECT SPInsurerId
      ,RetroProgramId
      --,SegregatedAccount
      --,ContractId
      --,InsurerId
      --,TrustBank
      --,CreateDate
      --,CreateUser
      --,ModifyDate
      --,ModifyUser
      --,IsActive
      --,IsDeleted
      ,CONVERT(BIGINT, RowVersion) AS RowVersion
      --,TrustAccountNumber
      --,FundsWithheldAccountNumber
      ,InitialCommutationDate
      ,FinalCommutationDate
  FROM dbo.SPInsurer
 WHERE IsActive = 1
   AND IsDeleted = 0";


        private const string GET_RETRO_INVESTOR = @"SELECT RetroInvestorId
      ,SPInsurerId
      --,Name
      ,Status
      ,TargetCollateral
      ,NotionalCollateral
      ,InvestmentEstimated
      ,InvestmentAuth
      ,InvestmentSigned
      ,InvestmentEstimatedAmt
      ,InvestmentAuthAmt
      ,InvestmentSignedAmt
      ,ExcludedFacilities
      ,ExcludedLayerSubNos
      ,ExcludedDomiciles
      ,IsFundsWithheld
      ,RetroCommissionId
      --,CreateDate
      --,CreateUser
      --,ModifyDate
      --,ModifyUser
      --,IsActive
      --,IsDeleted
      --,RuleDefs
      ,CONVERT(BIGINT, RowVersion) AS RowVersion
      ,ExcludedLayerIds
      ,TargetPremium
      ,Override
      ,ManagementFee
      ,ProfitComm
      ,PerformanceFee
      ,RHOE
      ,HurdleRate
      ,IsPortIn
      ,IsPortOut
      ,RetroBufferType
      ,CessionCapBufferPct
      ,RetroValuesToBuffer
      ,ExcludedContractType
  FROM dbo.RetroInvestor
 WHERE IsActive = 1
   AND IsDeleted = 0";

        private const string GET_RETRO_ZONE = @"SELECT --RetroZoneId,
       RetroProgramId
      --,Name
      --,ELLowerBound
      --,ELUpperBound
      --,ROLLowerBound
      --,ROLUpperBound
      ,Cession
      --,CessionCap
      --,CreateDate
      --,CreateUser
      --,ModifyDate
      --,ModifyUser
      --,IsActive
      --,IsDeleted
      --,RowVersion
      --,CessionCapAdjusted
      ,TopUpZoneId
      --,StartDate
  FROM dbo.RetroZone
 WHERE IsActive = 1
   AND IsDeleted = 0";

        private const string GET_LAYER_TOPUPZONE = @"SELECT LayerId
      ,TopUpZoneId
  FROM dbo.Layer
 WHERE IsActive = 1
   AND IsDeleted = 0
   AND TopUpZoneId IS NOT NULL";

        private const string GET_SUBMISSIONS = @"SELECT SubmissionId
      ,ProgramId
      ,RegisId
      ,BaseCurrency
      ,Currency
      ,FXRate
      ,FXDate
      ,TranType
      ,InceptionDefault
      ,UWYearDefault
      ,IsMultiyear
      ,IsCancellable
      --,ExpirationDefault
      --,QuoteDeadline
      --,AuthDeadline
      --,Arrived
      --,BrokerId
      --,BrokerContactId
      --,UnderwriterId
      --,ActuaryId
      --,AnalystId
      --,ModelerId
      --,RiskZoneId
      --,Notes
      --,UWNotes
      --,Correspondence
      --,StrategicNotes
      --,RefId
      --,Status
      --,IsRenewal
      --,DocStatus
      --,ModelingStatus
      --,Priority
      --,ExpiringSubmissionId
      --,Surplus
      --,ClientScore
      --,SubmissionWriteupId
      --,CreateDate
      --,CreateUser
      --,ModifyDate
      --,ModifyUser
      --,IsActive
      --,IsDeleted
      --,LegalTermsId
      --,PlacementYear
      --,ParentSubmissionId
      --,CedentAltName
      --,ModelingDeadline
      --,ModelingNotes
      --,DataLinkNotes
      --,MdlStatusDate
      --,ActuarialNotes
      --,RelshipUnderwriterId
      --,MarketShare
      --,CorreAuthDeadline
      --,CorreStatus
      --,ActuarialStatus
      --,SubmissionDataLinkNotes
      --,ActuarialDataLinkNotes
      --,ActuarialDeadline
      --,Source
      --,IsCorreInterest
      --,ActuarialPriority
      --,RegisSyncStatus
      --,LastRegisSyncByUserId
      --,LastRegisSyncDate
      --,ModelingComplexity
      --,ActuarialDataCheck
      --,ActuarialRanking
      --,IsActuarialDataCheckRequested
      ,CONVERT(BIGINT, RowVersion) AS RowVersion
      --,ERCLossViewArch
      --,ERCLossViewAir
      --,ERCLossViewRMS
      --,FxRateSBFUSD
      --,FxRateSBFGBP
      --,FxDateSBF
      --,IrisPolicyNumber
      --,RationaleQuote
      --,RationaleAuth
      --,RationaleSigned
      --,IrisSLA
      --,IsCollateralized
      --,BrokerRating
      --,BrokerRationale
      --,PNOCDays
      --,ClientAdvocacyRating
      --,ClientAdvocacyRationale
      --,LMXIndicator
      --,ActuaryPeerReviewerId
      --,ClientAdvocacyLink
      --,GroupBuyerId
      --,LocalBuyerId
      --,IsActuaryPeerReviewNotRequired
      --,CedeCoverageSelectionType
      --,DataScoreRating
  FROM dbo.Submission
 WHERE IsActive = 1
   AND IsDeleted = 0";

        private const string GET_RETRO_PROFILES = @"SELECT RetroProfileId
      ,Name
      ,RegisId
      ,ManagerId
      ,CompanyId
      ,OfficeId
      ,DeptId
      --,CreateDate
      --,CreateUser
      --,ModifyDate
      --,ModifyUser
      --,IsActive
      --,IsDeleted
      ,CONVERT(BIGINT, RowVersion) AS RowVersion
  FROM dbo.RetroProfile
 WHERE IsActive = 1
   AND IsDeleted = 0";

        private const string GET_FX_RATES = @"SELECT FXRateId
      ,BaseCurrency
      ,Currency
      ,FXDate
      ,Rate
      --,CreateDate
      --,CreateUser
      --,ModifyDate
      --,ModifyUser
      --,IsActive
      --,IsDeleted
      ,CONVERT(BIGINT, RowVersion) AS RowVersion
  FROM dbo.FXRate
 WHERE IsActive = 1
   AND IsDeleted = 0";

        private const string GET_LAYER_LOSS_ANALYSES = @"SELECT A.LossAnalysisId
     , A.LayerId
     , L.LossView
     , CONVERT(BIGINT, A.RowVersion) AS RowVersion
  FROM dbo.LayerLossAnalysis A
 INNER JOIN dbo.LossAnalysis L
    ON L.LossAnalysisId = A.LossAnalysisId
 WHERE A.IsActive = 1
   AND A.IsDeleted = 0
   AND L.IsActive = 1
   AND L.IsDeleted = 0
   AND L.LossView IN (1, 10, 3, 30, 4)";

        private const string GET_LAYER_LOSS_ANALYSES_INCREMENTAL = GET_LAYER_LOSS_ANALYSES + " AND CONVERT(BIGINT, A.RowVersion) > {0}";

        private const string GET_RETRO_LAYERS = @"SELECT A.LayerId, SI.RetroProgramId, MAX(CONVERT(BIGINT, A.RowVersion)) AS RowVersion 
  FROM dbo.retroallocation A
 INNER JOIN dbo.RetroInvestor I
    ON I.RetroInvestorId = A.RetroInvestorId
 INNER JOIN dbo.SpInsurer SI
    ON SI.SpInsurerId = I.SpInsurerId   
 WHERE CessionGross > 0
   AND A.IsActive = 1
   AND A.IsDeleted = 0
   AND I.IsActive = 1
   AND I.IsDeleted = 0   
   AND SI.IsActive = 1
   AND SI.IsDeleted = 0
 GROUP BY A.LayerId, SI.RetroProgramId";

        private const string GET_RETRO_LAYERS_INCREMENTAL = GET_RETRO_LAYERS + " HAVING MAX(CONVERT(BIGINT, A.RowVersion)) > {0}";

        private const string GET_PORTFOLIO_RETRO_LAYERS = @"SELECT DISTINCT
       P.PortLayerId
     , P.LayerId
     , P.PortfolioId
     , C.RetroProgramId
     , CONVERT(BIGINT, C.RowVersion) AS RowVersion 
  FROM dbo.PortLayer P
 INNER JOIN dbo.PortLayerCession C
    ON C.PortLayerId = P.PortLayerId 
 WHERE P.IsActive = 1
   AND P.IsDeleted = 0   
   AND C.IsActive = 1
   AND C.IsDeleted = 0";

        private const string GET_PORTFOLIO_RETRO_LAYERS_INCREMENTAL = GET_PORTFOLIO_RETRO_LAYERS + " HAVING MAX(CONVERT(BIGINT, C.RowVersion)) > {0}";

        #endregion Constants
    }
}
