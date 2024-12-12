
using Amazon.Runtime.Internal.Transform;
using System.Collections.Concurrent;

namespace Arch.ILS.EconomicModel
{
    public class RetroMetricsFactory
    {
        private readonly IRevoRepository _revoRepository;

        public RetroMetricsFactory(IRevoRepository revoRepository)        
        { 
            _revoRepository = revoRepository;
        }

        public Task<RetroSummaryMetrics> GetRetroMetrics(DateTime currentFxDate, ResetType resetType, bool useBoundFx = true, Currency baseCurrency = Currency.USD, HashSet<int> retroIdFilter = null, HashSet<ContractStatus> contractStatusesFilter = null, PremiumAllocationType premiumAllocationType = PremiumAllocationType.Linear, int maxDegreeOfParallelism = 8)
        {
            return Task.Factory.StartNew(() =>
            {
                if (premiumAllocationType != PremiumAllocationType.Linear)
                    throw new NotImplementedException($"Premium Allocation Type {premiumAllocationType}");
                var retroAllocationView = _revoRepository.GetRetroCessionView();
                var retroPrograms = _revoRepository.GetRetroPrograms();
                var layerDetails = _revoRepository.GetLayerDetails();
                var submissions = _revoRepository.GetSubmissions();
                var fxRates = _revoRepository.GetFXRates();
                var layerRetroPlacementsTask = _revoRepository.GetLayerRetroPlacements();

                Task.WaitAll(retroAllocationView, retroPrograms, layerDetails, submissions, fxRates, layerRetroPlacementsTask);
                ConcurrentBag<RetroLayerMetrics> retroLayerMetrics = new();
                ConcurrentDictionary<int, RetroMetrics> retroMetricsById = new();
                var levelLayerCessions = retroAllocationView.Result.GetLevelLayerCessions();
                var layerRetroPlacements = layerRetroPlacementsTask.Result.ToDictionary(x => (x.RetroProgramId, x.LayerId));

#if DEBUG
                foreach (var layerCession in levelLayerCessions)
#else
                Parallel.ForEach(levelLayerCessions, new ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism }, layerCession =>
#endif
                {
                    if (retroIdFilter != null && !retroIdFilter.Contains(layerCession.RetroProgramId))
#if DEBUG
                        continue;
#else
                    return;
#endif
                    if (!layerDetails.Result.TryGetValue(layerCession.LayerId, out var layerDetail))
#if DEBUG
                        continue;
#else
                    return;
#endif

                    if (contractStatusesFilter != null && !contractStatusesFilter.Contains(layerDetail.Status))
#if DEBUG
                        continue;
#else
                    return;
#endif
                    /*if (layerDetail.Status != ContractStatus.Bound
                        && layerDetail.Status != ContractStatus.Signed
                        && layerDetail.Status != ContractStatus.SignReady
                        // && layerDetail.Status != ContractStatus.Budget
                        )
                        continue;*/
                    if (!submissions.Result.TryGetValue(layerDetail.SubmissionId, out var submission))
#if DEBUG
                        continue;
#else
                    return;
#endif
                    var fxRate = RevoHelper.GetFxRate(useBoundFx, currentFxDate, baseCurrency.ToString(), submission, layerDetail, fxRates.Result);
                    var retroProgram = retroPrograms.Result[layerCession.RetroProgramId];

                    if (!retroMetricsById.TryGetValue(retroProgram.RetroProgramId, out var retroMetrics))
                    {
                        retroMetrics = new RetroMetrics(retroProgram.RetroProgramId)
                        {
                            RetroLevel = (byte)(retroProgram.RetroLevelType + 1),
                            RetroProgramType = retroProgram.RetroProgramType,
                            RetroInception = retroProgram.Inception,
                            RetroExpiration = retroProgram.Expiration,
                            DateLimits = new ConcurrentDictionary<DateTime, LimitMetrics>()
                        };
                        retroMetricsById.TryAdd(retroProgram.RetroProgramId, retroMetrics);
                    }

                    decimal netToGrossCession = (layerCession.PeriodCession.NetCession == decimal.Zero ? decimal.Zero : layerCession.PeriodCession.NetCession / layerCession.GrossCession);
                    decimal depositPremium = layerDetail.Premium
                        * (layerDetail.Placement == decimal.Zero ? decimal.Zero : 1 / layerDetail.Placement)
                        * (layerDetail.SignedShare > decimal.Zero ? layerDetail.SignedShare : layerDetail.EstimatedShare)
                        * fxRate
                        * (submission.TranType == TranType.Ceded ? -1 : (submission.TranType == TranType.Assumed ? 1 : 0));
                     decimal availablePremium = depositPremium * (decimal)(((layerCession.PeriodCession.EndInclusive - layerCession.PeriodCession.StartInclusive).TotalDays + 1) / ((layerDetail.Expiration - layerDetail.Inception).TotalDays + 1));/*linear allocation of premium*/
                    decimal cededPremium = availablePremium * layerCession.PeriodCession.NetCession;
                    retroMetrics.CededPremium += cededPremium;
                    decimal subjectPremium = availablePremium * netToGrossCession;
                    retroMetrics.SubjectPremium += subjectPremium;
                    decimal subjectPremiumPlaced = subjectPremium;
                    if (layerRetroPlacements.TryGetValue((layerCession.RetroProgramId, layerCession.LayerId), out LayerRetroPlacement layerRetroPlacement))
                        subjectPremiumPlaced *= (layerRetroPlacement?.Placement ?? decimal.One);
                    retroMetrics.SubjectPremiumPlaced += subjectPremiumPlaced;

                    decimal limit100Pct = GetLimit100Pct(layerDetail);
                    decimal depositLimit = limit100Pct
                        * (layerDetail.SignedShare > decimal.Zero ? layerDetail.SignedShare : layerDetail.EstimatedShare)
                        * fxRate
                        * (submission.TranType == TranType.Ceded ? -1 : (submission.TranType == TranType.Assumed ? 1 : 0));
                    decimal subjectLimit = depositLimit * netToGrossCession;
                    decimal subjectLimitPlaced = subjectLimit * (layerRetroPlacement?.Placement ?? decimal.One);
                    decimal cededLimit = depositLimit * layerCession.PeriodCession.NetCession;

                    var currentDate = layerCession.PeriodCession.StartInclusive;
                    var dateLimits = retroMetrics.DateLimits;
                    while (currentDate <= layerCession.PeriodCession.EndInclusive)
                    {
                        if (dateLimits.ContainsKey(currentDate))
                        {
                            LimitMetrics limitMetrics = dateLimits[currentDate];
                            limitMetrics.SubjectLimit += subjectLimit;
                            limitMetrics.SubjectLimitPlaced += subjectLimitPlaced;
                            limitMetrics.CededLimit += cededLimit;
                        }
                        else
                            dateLimits[currentDate] = new LimitMetrics(subjectLimit, subjectLimitPlaced, cededLimit);
                        currentDate = currentDate.AddDays(1);
                    }

                    retroLayerMetrics.Add(new RetroLayerMetrics(layerCession.RetroLevel, layerCession.RetroProgramId, retroProgram.RetroProgramType,
                        retroProgram.Inception, retroProgram.Expiration, layerCession.LayerId, layerDetail.Inception, layerDetail.Expiration, layerDetail.Status,
                        layerCession.PeriodCession.StartInclusive, layerCession.PeriodCession.EndInclusive, (layerRetroPlacement?.Placement ?? decimal.One), depositPremium,
                        subjectPremium, subjectPremiumPlaced, cededPremium, depositLimit, subjectLimit, subjectLimitPlaced, cededLimit, layerCession.GrossCession, layerCession.PeriodCession.NetCession));
                }
#if !DEBUG
                );
#endif

                return new RetroSummaryMetrics(retroMetricsById, retroLayerMetrics.ToList());
            });
        }

        private static decimal GetLimit100Pct(LayerDetail layerDetail)
        {
            return layerDetail.LimitBasis == LimitBasis.Aggregate ?
                layerDetail.AggLimit :
                (layerDetail.LimitBasis == LimitBasis.PerRisk || layerDetail.LimitBasis == LimitBasis.NonCATQuotaShare ?
                    layerDetail.RiskLimit :
                    layerDetail.OccLimit);
        }
    }
}
