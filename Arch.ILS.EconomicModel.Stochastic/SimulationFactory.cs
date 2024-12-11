
using Amazon.Runtime.Internal.Transform;
using Arch.ILS.DependencyEngine;
using Microsoft.Extensions.Azure;
using Mono.Unix;
using Service.Calculation;
using Service.DTO;
using System.Collections.Generic;
using System.Reflection.Emit;
using System.Text;

namespace Arch.ILS.EconomicModel.Stochastic
{
    public class SimulationFactory
    {
        public const string EXPORT_FOLDER = "C:\\Data\\Exports\\";
        public const string EXPORT_EROSION_FOLDER = "C:\\Data\\Exports Erosion\\";
        public const string EXPORT_FOLDER_ALT = "//C$/Data/Exports/";
        public const string EXPORT_EROSION_FOLDER_ALT = "//C$/Data/Exports Erosion/";
        public const string RECALCULATED_LAYER_FILE_FORMAT = "RECALCULATED_YELT_{0}_{1}_{2}_{3}.csv";
        public const string CONDITIONAL_LAYER_FILE_FORMAT = "CONDITIONAL_YELT_{0}_{1}_{2}_{3}.csv";
        public const string RETRO_REGIS_IBNR_ASSUMED_FORMAT = "RETRO_REGIS_IBNR_ASSUMED_{0}_{1}_AsAt_{2}.csv";
        public const string LOG_FORMAT = "LOG_{0}_{1}_AsAt_{2}.csv";

        private readonly IRevoRepository _revoRepository;
        private readonly IRevoLayerLossRepository _revoLayerLossRepository;
        private readonly IRevoGULossRepository _revoGULossRepository;
        private readonly IActuarialStochasticRepository _actuarialStochasticRepository;
        private readonly IMixedRepository _mixedSnowflakeRepository;

        public SimulationFactory(IRevoRepository revoRepository, IRevoLayerLossRepository revoLayerLossRepository, IRevoGULossRepository revoGULossRepository, IActuarialStochasticRepository actuarialStochasticRepository, IMixedRepository mixedSnowflakeRepository) 
        {
            _revoRepository = revoRepository;
            _revoLayerLossRepository = revoLayerLossRepository;
            _revoGULossRepository = revoGULossRepository;
            _actuarialStochasticRepository = actuarialStochasticRepository;
            _mixedSnowflakeRepository = mixedSnowflakeRepository;
        }

        public void GetConditionYelt(bool applyErosion, int calculationId, string name, int retroProgramId, DateTime asAtDate, HashSet<RevoLossViewType> revoLossViewTypes)
        {
            var layersTask = _revoRepository.GetLayerDetails();
            var submissionsTask = _revoRepository.GetSubmissions();
            var reinstatementsTask = _revoRepository.GetLayerReinstatements();
            var pxSectionsByLayerIdTask = _revoRepository.GetPXSections();
            var submissionGUAnalysesTask = _revoRepository.GetSubmissionGUAnalyses();
            DateTime quarterEndDate = GetQuarterEndDate(asAtDate);
            int acctGPeriod = GetAcctGPeriod(asAtDate);
            List<LayerActualMetrics> retroLayerActualITDMetrics = _mixedSnowflakeRepository.GetRetroLayerActualITDMetrics(retroProgramId, acctGPeriod).ToList();
            var layerActualLoss = retroLayerActualITDMetrics.GroupBy(x => x.LayerId).ToDictionary(k => k.Key, v => v.Select(vv => vv.UltimateLoss).Sum());
            var retroLayerIds = retroLayerActualITDMetrics.Select(x => x.LayerId).ToHashSet();
            if (applyErosion)
                Export(Path.Combine(EXPORT_EROSION_FOLDER, string.Format(RETRO_REGIS_IBNR_ASSUMED_FORMAT, calculationId, acctGPeriod, asAtDate.ToString("yyyy-MM-dd"))), calculationId, retroLayerActualITDMetrics);
            SimulationLog simulationLog = new();
            Task.WaitAll(layersTask, submissionsTask, reinstatementsTask, pxSectionsByLayerIdTask, submissionGUAnalysesTask);
            var layers = layersTask.Result;
            var submissions = submissionsTask.Result;
            var reinstatements = reinstatementsTask.Result.GroupBy(x => x.LayerId).ToDictionary(k => k.Key, v => v.OrderBy(o => o.ReinstatementOrder).ToArray());
            var pxSectionsByLayerId = pxSectionsByLayerIdTask.Result.Where(x => retroLayerIds.Contains(x.LayerId) || retroLayerIds.Contains(x.PXLayerId)).GroupBy(g => g.LayerId).ToDictionary(k => k.Key, v => v.ToList());
            var pxSectionsByPXLayerId = pxSectionsByLayerId.Values.SelectMany(s => s).GroupBy(g => g.PXLayerId).ToDictionary(k => k.Key, v => v.ToList());
            var submissionGUAnalyses = submissionGUAnalysesTask.Result.ToDictionary(k => (k.SubmissionId, k.GUAnalysisId));
            UndirectedGraph<int> layerGraph = new UndirectedGraph<int>(retroLayerIds.Union(pxSectionsByLayerId.Keys).Union(pxSectionsByPXLayerId.Keys));
            foreach (var dependency in pxSectionsByLayerId.Values.SelectMany(s => s))
                layerGraph.Add(dependency.PXLayerId, dependency.LayerId);
            var layerGroups = layerGraph.GetIslands().Select(x => x.ToArray()).OrderBy(o => o[0]).ToArray();

            Dictionary<int, Dictionary<RevoLossViewType, List<LayerLossAnalysisExtended>>> lossAnalysesByLayerLossView = _revoRepository.GetLayerLossAnalysesExtended().Result
                .Where(f => revoLossViewTypes.Contains(f.LossView) && f.GUAnalysisId != null)
                .GroupBy(g => g.LayerId)
                .ToDictionary(k => k.Key, v => v
                    .GroupBy(gg => gg.LossView)
                    .ToDictionary(kk => kk.Key, vv => vv.OrderByDescending(o => o.RowVersion).ToList()));

            foreach (RevoLossViewType lossViewType in revoLossViewTypes)
            {
#if DEBUG
                for (int i = 0; i < layerGroups.Length; ++i)
#else
                Parallel.For(0, layerGroups.Length, i =>
#endif
                {
                    try
                    {
                        var layerGroup = layerGroups[i];
                        // if(layerGroup.Contai)

                        //HashSet<int> h = new HashSet<int>
                        //{
                        //    //136866,
                        //    //137821,//x
                        //    141578,
                        //    //144674,//x
                        //    //145275,//x
                        //    //145415,
                        //    //146100,
                        //    //147252,
                        //    //149033,//x
                        //    //153113,
                        //    //153687,
                        //    //157772,//small diff
                        //    //158305,//x
                        //    //158306,//x
                        //    //159513,
                        //    //159514
                        //};

                        //if (!h.Intersect(layerGroup).Any())
                        //    continue;

                        //if(!layerGroup.Contains(143367))
                        //{
                        //    continue;
                        //}

                        Dictionary<int, LayerLossAnalysisExtended> layerLossAnalyses = new Dictionary<int, LayerLossAnalysisExtended>();

                        /*Handle cases where non-modelled layer loss cannot be found in the Ground-up modelling*/
                        DirectedGraph<int> dependencyGraph = new DirectedGraph<int>(layerGroup);
                        foreach (int layerId in layerGroup)
                        {
                            if (pxSectionsByLayerId.TryGetValue(layerId, out var layerSections))
                                foreach (var layerSection in layerSections)
                                    dependencyGraph.AddEdge(layerSection.LayerId, layerSection.PXLayerId);

                            if (!lossAnalysesByLayerLossView.TryGetValue(layerId, out var lossViewTypeAnalyses)
                                || !lossViewTypeAnalyses.TryGetValue(lossViewType, out var analyses))
                            {
                                StringBuilder message = new StringBuilder($"No Loss Analysis associated with layer {layerId} for loss view type {lossViewType.ToString()}.");
                                message.Append($"The status of this layer is {layers[layerId].Status.ToString()}.");
                                if(layerActualLoss.TryGetValue(layerId, out double layerActualITDLoss))
                                    message.Append($"Regis has recorded USD{layerActualITDLoss} loss for this layer.");
                                else message.Append($"Regis has recorded no loss for this layer.");
                                Console.WriteLine(message);
                                simulationLog.Append(LogLevel.Warning, layerId, message.ToString(), retroLayerIds.Contains(layerId));
                                continue;
                            }

                            LayerLossAnalysisExtended latestLossAnalysis = analyses.First();
                            layerLossAnalyses[layerId] = latestLossAnalysis;
                        }

                        Dictionary<int, List<RevoLayerYeltEntry>> originalLayerYelts = new Dictionary<int, List<RevoLayerYeltEntry>>();
                        Dictionary<int, List<DtoLayeringStcInput>> guYelts = new Dictionary<int, List<DtoLayeringStcInput>>();
                        var sortedDependencies = dependencyGraph.TopologicalSort();
                        bool anyLayerOnlyPerils = false;
                        Dictionary<int, HashSet<byte>> layerOnlyPerils = new Dictionary<int, HashSet<byte>>();
                        foreach (var layerId in sortedDependencies)
                        {
                            LayerLossAnalysisExtended latestLossAnalysis = layerLossAnalyses[layerId];
                            var originalLayerYelt = _revoLayerLossRepository.GetRevoLayerYeltEntries(latestLossAnalysis.LossAnalysisId, layerId, false).ToList();
                            originalLayerYelts[layerId] = originalLayerYelt;
                            HashSet<byte> distinctLayerPerils = originalLayerYelt.Select(x => x.PerilId).ToHashSet();
                            HashSet<byte> distinctGUPerils = new HashSet<byte>();
                            int guAnalysisId = (int)latestLossAnalysis.GUAnalysisId;
                            LayerDetail layerDetail = layers[layerId];

                            List<DtoLayeringStcInput> guEntries = new();
                            guYelts[layerId] = guEntries;

                            /*Layering Input*/
                            foreach (var entry in _revoGULossRepository.GetRevoGUYelt(guAnalysisId).Result)
                            {
                                distinctGUPerils.Add((byte)entry.Peril);
                                guEntries.Add(new DtoLayeringStcInput
                                {
                                    Year = entry.Year,
                                    Day = entry.Day,
                                    Peril = entry.Peril.ToString(),
                                    EventId = entry.EventId,
                                    LayerId = layerId,
                                    GULossInLayerCurrency = (decimal)(entry.Loss * latestLossAnalysis.GetTotalLoadxLAE(entry.Peril)
                                        * (submissionGUAnalyses.TryGetValue((layerDetail.SubmissionId, guAnalysisId), out var submissionGUAnalysis) ? submissionGUAnalysis.FXRate : 1.0)),
                                    LAE = (decimal)latestLossAnalysis.GetLAELoad(entry.Peril)
                                });
                            }

                            layerOnlyPerils[layerId] = distinctLayerPerils.Except(distinctGUPerils).ToHashSet();
                            if (layerOnlyPerils[layerId].Any())
                                anyLayerOnlyPerils = true;
                        }

                        if (anyLayerOnlyPerils)
                        {
                            Dictionary<int, List<DtoLayeringStcOutput>> layeringOutputs = new Dictionary<int, List<DtoLayeringStcOutput>>();
                            HashSet<int> exhaustedLayers = new HashSet<int>();
                            foreach (var layerId in sortedDependencies)
                            {
                                /**1. First model the layer only with layers it depends on (not layers that depend on it) so that inuring or sources losses to the GU losses are correctly calculated and without agg terms**/
                                List<DtoLayerStcInput> occLayers = new();
                                List<DtoLayeringStcInput> occLayering = new(guYelts[layerId]);
                                List<DtoLayeringSection> occSections = new();
                                DtoLayeringStcRequest occRequest = new(true, true, occLayering, occLayers, occSections);
                                LayerDetail layerDetail = layers[layerId];
                                DateTime inceptionDate = applyErosion ? (quarterEndDate > layerDetail.Inception ? quarterEndDate : layerDetail.Inception)  : layerDetail.Inception;
                                DateTime expirationDate = layerDetail.Expiration;

                                if(inceptionDate > expirationDate)
                                {
                                    string message = $"No valid in-force period for layer {layerId} - LayerInception {layerDetail.Inception} - LayerExpiration {layerDetail.Expiration} - Quarter End {quarterEndDate}";
                                    Console.WriteLine($"{i + 1} / {layerGroups.Length} - {message}");
                                    simulationLog.Append(LogLevel.Warning, layerId, message, retroLayerIds.Contains(layerId));
                                    continue;
                                }

                                /*Layer*/
                                occLayers.Add(new DtoLayerStcInput
                                {
                                    LayerId = layerId,
                                    OccLimit = GetOccLimit(layerDetail),
                                    OccRetention = GetOccRetention(layerDetail),
                                    AggLimit = 0,//layerDetail.AggLimit,
                                    AggRetention = 0,//layerDetail.AggRetention,
                                    Franchise = layerDetail.Franchise,
                                    FranchiseReverse = layerDetail.FranchiseReverse,
                                    Placement = decimal.One,//layerDetail.Placement,
                                    IsFHCF = layerDetail.LayerType == LayerType.FHCF,
                                    Currency = submissions[layerDetail.SubmissionId].Currency,
                                    Premium = 0,
                                    Reinstatements = null
                                });

                                /*Sections*/
                                HashSet<DtoLayeringSection> occRequestSections = new HashSet<DtoLayeringSection>();
                                if (pxSectionsByLayerId.TryGetValue(layerId, out var pxSections))
                                {
                                    foreach (var section in pxSections)
                                    {
                                        occRequestSections.Add(new DtoLayeringSection
                                        {
                                            LayerId = section.LayerId,
                                            SectionId = section.PXLayerId,
                                            RollUpType = (Service.Enums.ERollUpType)(byte)section.Rollup,
                                            FXRateToParent = (decimal)section.FXToParent
                                        });

                                        var sectionDetail = layers[section.PXLayerId];
                                        occRequest.Layers.Add(new DtoLayerStcInput
                                        {
                                            LayerId = section.PXLayerId,
                                            OccLimit = decimal.MaxValue,
                                            OccRetention = 0,//sectionLayeringOutput.OccRetention,
                                            AggLimit = 0,//sectionDetail.AggLimit,
                                            AggRetention = 0,//sectionDetail.AggRetention,
                                            Franchise = 0,
                                            FranchiseReverse = 0,
                                            Placement = decimal.One,//sectionDetail.Placement,
                                            IsFHCF = sectionDetail.LayerType == LayerType.FHCF,
                                            Currency = submissions[sectionDetail.SubmissionId].Currency,
                                            Premium = 0,
                                            InceptionDate =  inceptionDate,
                                            ExpirationDate = expirationDate,
                                            Reinstatements = null
                                        });

                                        var sectionLayeringOutput = layeringOutputs[section.PXLayerId];
                                        foreach (var entry in sectionLayeringOutput)
                                        {
                                            occLayering.Add(new DtoLayeringStcInput
                                            {
                                                Year = entry.Year,
                                                Day = entry.Day,
                                                Peril = entry.Peril.ToString(),
                                                EventId = entry.EventId,
                                                LayerId = entry.LayerId,
                                                GULossInLayerCurrency = entry.LayerLoss100Pct ?? decimal.Zero,
                                                LAE = decimal.One
                                            });
                                        }
                                    }

                                    occSections.AddRange(occRequestSections);
                                }
                                LayeringEngine occLayeringEngine = new LayeringEngine();
                                DtoLayeringStcResponse occResponse = occLayeringEngine.Compute(occRequest);

                                /**2. Combine response from Layer Occurrence losses obtained from GU losses with the non-modelled layer losses and aggregate them together**/
                                List<DtoLayerStcInput> aggLayers = new();
                                List<DtoLayeringStcInput> aggLayering = new();
                                List<DtoLayeringSection> aggSections = new();
                                DtoLayeringStcRequest aggRequest = new(true, true, aggLayering, aggLayers, aggSections);

                                /*Layering*/
                                foreach (var entry in occResponse.Layers.Where(x => x.LayerId == layerId))
                                {
                                    if (entry.LayerLoss100Pct != null && entry.LayerLoss100Pct != decimal.Zero)
                                        aggLayering.Add(new DtoLayeringStcInput
                                        {
                                            Year = entry.Year,
                                            Day = entry.Day,
                                            Peril = entry.Peril.ToString(),
                                            EventId = entry.EventId,
                                            LayerId = entry.LayerId,
                                            GULossInLayerCurrency = (decimal)entry.LayerLoss100Pct,
                                            LAE = decimal.One
                                        });
                                }

                                var missingLayerPerils = layerOnlyPerils[layerId];
                                decimal occLimit = GetOccLimit(layerDetail);
                                foreach (var entry in originalLayerYelts[layerId])
                                {
                                    if (missingLayerPerils.Contains(entry.PerilId))
                                    {
                                        aggLayering.Add(new DtoLayeringStcInput
                                        {
                                            Year = entry.Year,
                                            Day = entry.Day,
                                            Peril = ((RevoPeril)entry.PerilId).ToString(),
                                            EventId = entry.EventId,
                                            LayerId = layerId,
                                            GULossInLayerCurrency = (decimal)entry.LossPct * occLimit,
                                            LAE = decimal.One
                                        });
                                    }
                                }

                                /*Layer*/
                                double erosion = 0;
                                bool layerApplyErosion = applyErosion && layerActualLoss.TryGetValue(layerId, out erosion);
                                decimal remainingLimit = Math.Max(layerDetail.AggLimit - (decimal)erosion, 0.0m);
                                if (remainingLimit > 0)
                                {
                                    decimal layerOccLimit = GetOccLimit(layerDetail);
                                    decimal premium = layerDetail.Placement == decimal.Zero ? decimal.Zero : layerDetail.Premium / layerDetail.Placement;
                                    var layerReinstatements = reinstatements.TryGetValue(layerId, out var layerReinstements)
                                        ? layerReinstements.Select(r => new DtoReinstatement
                                        {
                                            ReinstatementId = r.ReinstatementId,
                                            ReinstatementOrder = r.ReinstatementOrder,
                                            Quantity = r.Quantity,
                                            PremiumShare = r.PremiumShare,
                                            BrokeragePercentage = r.BrokeragePercentage
                                        }).ToArray()
                                        : null;
                                    if (layerApplyErosion && layerReinstatements != null)
                                        layerReinstatements = Reinstatements.GetErodedReinstatement(layerReinstatements, layerOccLimit, premium, (decimal)erosion);

                                    aggLayers.Add(new DtoLayerStcInput
                                    {
                                        LayerId = layerId,
                                        OccLimit = layerOccLimit,
                                        OccRetention = 0,
                                        AggLimit = remainingLimit,
                                        AggRetention = layerApplyErosion && erosion > 0 ? 0 : layerDetail.AggRetention,
                                        Franchise = layerDetail.Franchise,
                                        FranchiseReverse = layerDetail.FranchiseReverse,
                                        Placement = layerDetail.Placement,
                                        IsFHCF = layerDetail.LayerType == LayerType.FHCF,
                                        Currency = submissions[layerDetail.SubmissionId].Currency,
                                        Premium = premium,
                                        InceptionDate = inceptionDate,
                                        ExpirationDate = expirationDate,
                                        Reinstatements = layerReinstatements
                                    });

                                    /*Sections */
                                    //No section needed
                                    LayeringEngine aggLayeringEngine = new LayeringEngine();
                                    DtoLayeringStcResponse aggResponse = aggLayeringEngine.Compute(aggRequest);
                                    layeringOutputs.Add(layerId, aggResponse.Layers);
                                }
                                else exhaustedLayers.Add(layerId);

                            }
                            HashSet<int> selectedLayerIds = new HashSet<int>(retroLayerIds.Intersect(layeringOutputs.Keys));
                            foreach (int selectedLayerId in selectedLayerIds)
                            {
                                if(exhaustedLayers.Contains(selectedLayerId))
                                {
                                    string message = $"LayerId {selectedLayerId}  - Limit exhausted";
                                    Console.WriteLine($"{i + 1}/{layerGroups.Length} - {message}");
                                    Console.WriteLine(message);
                                    simulationLog.Append(LogLevel.Warning, selectedLayerId, message.ToString(), retroLayerIds.Contains(selectedLayerId));
                                }
                                else
                                {
                                    var layer = layers[selectedLayerId];
                                    var layerLossAnalysis = layerLossAnalyses[selectedLayerId];
                                    var originalLayerYelt = originalLayerYelts[selectedLayerId];
                                    var originalSumLossPct = originalLayerYelt.Select(x => x.LossPct).Sum();
                                    var originalCount = originalLayerYelt.Count();
                                    var recalculatedLayerYelt = layeringOutputs[selectedLayerId].Where(x => x.LayerLoss100Pct != 0).ToList();
                                    var recalculatedSumLossPct = recalculatedLayerYelt.Select(x => x.LayerLoss100Pct / x.OccLimit).Sum();
                                    var recalculatedCount = recalculatedLayerYelt.Count();
                                    Reinstatement[] layerReinstements = null;
                                    reinstatements.TryGetValue(selectedLayerId, out layerReinstements);
                                    double reinstCount = layerReinstements == null ? 0 : layerReinstements.Select(x => x.Quantity).Sum();
                                    var originalLossesByYear = originalLayerYelt.GroupBy(g => g.Year).ToDictionary(z => z.Key, v => v.OrderBy(x => x.Day).ToList());
                                    var recalculatedLossesByYear = recalculatedLayerYelt.GroupBy(g => g.Year).ToDictionary(z => z.Key, v => v.OrderBy(x => x.Day).ToList());

                                    if (applyErosion || Math.Abs(originalSumLossPct - (double)(recalculatedSumLossPct ?? 0)) < 0.01)
                                    {
                                        string filePath = applyErosion ? 
                                            Path.Combine(EXPORT_EROSION_FOLDER, string.Format(CONDITIONAL_LAYER_FILE_FORMAT, calculationId, layer.LayerId, layerLossAnalysis.LossAnalysisId, layerLossAnalysis.GUAnalysisId)) : 
                                            Path.Combine(EXPORT_FOLDER, string.Format(RECALCULATED_LAYER_FILE_FORMAT, calculationId, layer.LayerId, layerLossAnalysis.LossAnalysisId, layerLossAnalysis.GUAnalysisId));
                                        //string filePathSnowflake = Path.Combine(applyErosion ? EXPORT_EROSION_FOLDER_ALT : EXPORT_FOLDER_ALT, string.Format(RECALCULATED_LAYER_FILE_FORMAT, calculationId, layer.LayerId, layerLossAnalysis.LossAnalysisId, layerLossAnalysis.GUAnalysisId));
                                        Export(filePath, calculationId, layer.LayerId, layerLossAnalysis.LossAnalysisId, (int)layerLossAnalysis.GUAnalysisId, recalculatedLayerYelt);
                                        _mixedSnowflakeRepository.BulkCopyRecalculatedYelt(filePath);
                                    }

                                    if(!applyErosion)
                                    {
                                        string message = $"LayerId {selectedLayerId}  - LossAnalysisId {layerLossAnalysis.LossAnalysisId} - Actual Sum LossPct {originalSumLossPct} vs recalculated {recalculatedSumLossPct} - Actual Count {originalCount} vs recalculated {recalculatedCount} - Dependencies Count: {layerGroup.Length} - GUAnalysisId:{layerLossAnalysis.GUAnalysisId} - LimitBasis: {layer.LimitBasis.ToString()} - LayerType: {layer.LayerType.ToString()} - OccLimit: {layer.OccLimit} - AggLimit: {layer.AggLimit} - RiskLimit:{layer.RiskLimit} - ReinsCount:{reinstCount}";
                                        Console.WriteLine($"{i + 1}/{layerGroups.Length} - {message}");
                                        simulationLog.Append(LogLevel.Warning, selectedLayerId, message, retroLayerIds.Contains(selectedLayerId));
                                    }
                                }
                            }
                        }
                        else
                        {
                            HashSet<int> exhaustedLayers = new HashSet<int>();
                            DtoLayeringStcRequest request = new DtoLayeringStcRequest() { ApplyLossAggregation = true, FilterOutEventsOutsideOfLayerInForcePeriod = true };
                            HashSet<DtoLayeringSection> sections = new HashSet<DtoLayeringSection>();
                            for (int j = 0; j < layerGroup.Length; ++j)
                            {
                                int layerId = layerGroup[j];
                                LayerDetail layerDetail = layers[layerId];
                                DateTime inceptionDate = applyErosion ? (quarterEndDate > layerDetail.Inception ? quarterEndDate : layerDetail.Inception) : layerDetail.Inception;
                                DateTime expirationDate = layerDetail.Expiration;

                                if (inceptionDate > expirationDate)
                                {
                                    string message = $"No valid in-force period for layer {layerId} - LayerInception {layerDetail.Inception} - LayerExpiration {layerDetail.Expiration} - Quarter End {quarterEndDate}";
                                    Console.WriteLine($"{i + 1} / {layerGroups.Length} - {message}");
                                    simulationLog.Append(LogLevel.Warning, layerId, message, retroLayerIds.Contains(layerId));
                                    continue;
                                }

                                /*Layer*/
                                double erosion = 0;
                                bool layerApplyErosion = applyErosion && layerActualLoss.TryGetValue(layerId, out erosion);
                                decimal remainingLimit = Math.Max(layerDetail.AggLimit - (decimal)erosion, 0.0m);
                                if (remainingLimit > 0)
                                {
                                    decimal layerOccLimit = GetOccLimit(layerDetail);
                                    decimal premium = layerDetail.Placement == decimal.Zero ? decimal.Zero : layerDetail.Premium / layerDetail.Placement;
                                    var layerReinstatements = reinstatements.TryGetValue(layerId, out var layerReinstements)
                                                ? layerReinstements.Select(r => new DtoReinstatement
                                                {
                                                    ReinstatementId = r.ReinstatementId,
                                                    ReinstatementOrder = r.ReinstatementOrder,
                                                    Quantity = r.Quantity,
                                                    PremiumShare = r.PremiumShare,
                                                    BrokeragePercentage = r.BrokeragePercentage
                                                }).ToArray()
                                                : null;
                                    if (layerApplyErosion && layerReinstatements != null)
                                        layerReinstatements = Reinstatements.GetErodedReinstatement(layerReinstatements, layerOccLimit, premium, (decimal)erosion);

                                    request.Layers.Add(new DtoLayerStcInput
                                    {
                                        LayerId = layerId,
                                        OccLimit = layerOccLimit,
                                        OccRetention = GetOccRetention(layerDetail),
                                        AggLimit = remainingLimit,
                                        AggRetention = layerApplyErosion && erosion > 0 ? 0 : layerDetail.AggRetention,
                                        Franchise = layerDetail.Franchise,
                                        FranchiseReverse = layerDetail.FranchiseReverse,
                                        Placement = layerDetail.Placement,
                                        IsFHCF = layerDetail.LayerType == LayerType.FHCF,
                                        Currency = submissions[layerDetail.SubmissionId].Currency,
                                        Premium = premium,
                                        InceptionDate = inceptionDate,
                                        ExpirationDate = expirationDate,
                                        Reinstatements = layerReinstatements
                                    });

                                    /*Sections*/
                                    if (pxSectionsByLayerId.TryGetValue(layerId, out var pxSections))
                                    {
                                        foreach (var section in pxSections)
                                            sections.Add(new DtoLayeringSection
                                            {
                                                LayerId = section.LayerId,
                                                SectionId = section.PXLayerId,
                                                RollUpType = (Service.Enums.ERollUpType)(byte)section.Rollup,
                                                FXRateToParent = (decimal)section.FXToParent
                                            });
                                    }

                                    /*Layering Input*/
                                    LayerLossAnalysisExtended latestLossAnalysis = layerLossAnalyses[layerId];
                                    int guAnalysisId = (int)latestLossAnalysis.GUAnalysisId;
                                    foreach (var entry in _revoGULossRepository.GetRevoGUYelt(guAnalysisId).Result)
                                    {
                                        request.LayerEvents.Add(new DtoLayeringStcInput
                                        {
                                            Year = entry.Year,
                                            Day = entry.Day,
                                            Peril = entry.Peril.ToString(),
                                            EventId = entry.EventId,
                                            LayerId = layerId,
                                            GULossInLayerCurrency = (decimal)(entry.Loss * latestLossAnalysis.GetTotalLoadxLAE(entry.Peril)
                                                * (submissionGUAnalyses.TryGetValue((layerDetail.SubmissionId, guAnalysisId), out var submissionGUAnalysis) ? submissionGUAnalysis.FXRate : 1.0)),
                                            LAE = (decimal)latestLossAnalysis.GetLAELoad(entry.Peril)
                                        });
                                    }
                                }
                                else exhaustedLayers.Add(layerId);
                            }

                            request.Sections.AddRange(sections);
                            LayeringEngine layeringEngine = new LayeringEngine();
                            DtoLayeringStcResponse response = layeringEngine.Compute(request);
                            HashSet<int> selectedLayerIds = new HashSet<int>(retroLayerIds.Intersect(request.Layers.Select(x => x.LayerId)));
                            foreach (int selectedLayerId in selectedLayerIds)
                            {
                                if(exhaustedLayers.Contains(selectedLayerId))
                                {
                                    string message = $"LayerId {selectedLayerId}  - Limit exhausted";
                                    Console.WriteLine($"{i + 1} / {layerGroups.Length} - {message}");
                                    simulationLog.Append(LogLevel.Warning, selectedLayerId, message, retroLayerIds.Contains(selectedLayerId));                                    
                                }
                                else
                                {
                                    var layer = layers[selectedLayerId];
                                    var layerLossAnalysis = layerLossAnalyses[selectedLayerId];
                                    var originalLayerYelt = _revoLayerLossRepository.GetRevoLayerYeltEntries(layerLossAnalysis.LossAnalysisId, selectedLayerId, false).ToList();
                                    var originalSumLossPct = originalLayerYelt.Select(x => x.LossPct).Sum();
                                    var originalCount = originalLayerYelt.Count();
                                    var recalculatedLayerYelt = response.Layers.Where(x => x.LayerId == selectedLayerId && x.LayerLoss100Pct != 0).ToList();
                                    var recalculatedSumLossPct = recalculatedLayerYelt.Select(x => x.LayerLoss100Pct / x.OccLimit).Sum();
                                    var recalculatedCount = recalculatedLayerYelt.Count();
                                    Reinstatement[] layerReinstements = null;
                                    reinstatements.TryGetValue(selectedLayerId, out layerReinstements);
                                    double reinstCount = layerReinstements == null ? 0 : layerReinstements.Select(x => x.Quantity).Sum();
                                    var originalLossesByYear = originalLayerYelt.GroupBy(g => g.Year).ToDictionary(z => z.Key, v => v.OrderBy(x => x.Day).ToList());
                                    var recalculatedLossesByYear = recalculatedLayerYelt.GroupBy(g => g.Year).ToDictionary(z => z.Key, v => v.OrderBy(x => x.Day).ToList());

                                    if (applyErosion || Math.Abs(originalSumLossPct - (double)(recalculatedSumLossPct ?? 0)) < 0.01)
                                    {
                                        string filePath = applyErosion ?
                                            Path.Combine(EXPORT_EROSION_FOLDER, string.Format(CONDITIONAL_LAYER_FILE_FORMAT, calculationId, layer.LayerId, layerLossAnalysis.LossAnalysisId, layerLossAnalysis.GUAnalysisId)) :
                                            Path.Combine(EXPORT_FOLDER, string.Format(RECALCULATED_LAYER_FILE_FORMAT, calculationId, layer.LayerId, layerLossAnalysis.LossAnalysisId, layerLossAnalysis.GUAnalysisId));
                                        Export(filePath, calculationId, layer.LayerId, layerLossAnalysis.LossAnalysisId, (int)layerLossAnalysis.GUAnalysisId, recalculatedLayerYelt);
                                        _mixedSnowflakeRepository.BulkCopyRecalculatedYelt(filePath);
                                    }

                                    string message = $"LayerId {selectedLayerId}  - LossAnalysisId {layerLossAnalysis.LossAnalysisId} - Actual Sum LossPct {originalSumLossPct} vs recalculated {recalculatedSumLossPct} - Actual Count {originalCount} vs recalculated {recalculatedCount} - Dependencies Count: {layerGroup.Length} - GUAnalysisId:{layerLossAnalysis.GUAnalysisId} - LimitBasis: {layer.LimitBasis.ToString()} - LayerType: {layer.LayerType.ToString()} - OccLimit: {layer.OccLimit} - AggLimit: {layer.AggLimit} - RiskLimit:{layer.RiskLimit} - ReinsCount:{reinstCount}";
                                    Console.WriteLine($"{i + 1} / {layerGroups.Length} - {message}");
                                    simulationLog.Append(LogLevel.Warning, selectedLayerId, message, retroLayerIds.Contains(selectedLayerId));
                                }
                            }
                        }
                    }
                    catch(Exception e)
                    {
                        string message = $"Error {e.ToString()}";
                        Console.WriteLine($"{i + 1} / {layerGroups.Length} - {message}");
                        simulationLog.Append(LogLevel.Error, message);
                    }
                    finally
                    {
                    }
                }
#if !DEBUG
                );
#endif
                simulationLog.Export(Path.Combine(applyErosion ? EXPORT_EROSION_FOLDER : EXPORT_FOLDER, string.Format(LOG_FORMAT, calculationId, acctGPeriod, asAtDate.ToString("yyyy-MM-dd"))));
            }
        }

        private static decimal GetOccLimit(LayerDetail layerDetail)
        {
            return layerDetail.LimitBasis switch
            {
                LimitBasis.Aggregate => layerDetail.AggLimit,
                LimitBasis.PerRisk or LimitBasis.NonCATQuotaShare => layerDetail.RiskLimit,
                _ => layerDetail.OccLimit
            };
        }

        private static decimal GetOccRetention(LayerDetail layerDetail)
        {
            return layerDetail.LimitBasis switch
            {
                LimitBasis.Aggregate => layerDetail.AggRetention,
                LimitBasis.PerRisk or LimitBasis.NonCATQuotaShare => layerDetail.RiskRetention,
                _ => layerDetail.OccRetention
            };
        }

        private void Export(string filePath, int calculationId, int layerId, int lossAnalysisId, int guLossAnalysisId, IEnumerable<DtoLayeringStcOutput> layeringStcOutputs)
        {
            using (FileStream fs = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None))
            using (StreamWriter sw = new StreamWriter(fs))
            {
                sw.WriteLine("CALCULATIONID,LOSSANALYSISID,LAYERID,EVENTID,PERIL,YEAR,DAY,LOSSPCT,RPPCT,RBPCT,LOSS,RP,RB");
                foreach(DtoLayeringStcOutput l in layeringStcOutputs)
                {
                    sw.WriteLine($"{calculationId},{lossAnalysisId},{layerId},{l.EventId},{l.Peril},{l.Year},{l.Day},{l.LayerLoss100Pct / l.OccLimit},{l.ReinstatementPremium100Pct / l.OccLimit},{l.ReinstatementBrokerage100Pct / l.OccLimit},{l.LayerLoss100Pct},{l.ReinstatementPremium100Pct},{l.ReinstatementBrokerage100Pct}");
                }

                sw.Flush();
            }
        }

        private void Export(string filePath, int calculationId, List<LayerActualMetrics> retroLayerActualITDMetrics)
        {
            using (FileStream fs = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None))
            using (StreamWriter sw = new StreamWriter(fs))
            {
                sw.WriteLine("CALCULATIONID,MASTERKEY,MASTERKEYFROM,LAYERID,SUBMISSIONID,ISMULTIYEAR,ISCANCELLABLE,UWYEAR,ACCTGPERIOD,SEGMENT,PERSPECTIVETYPE,CURRENCY,FACILITY,WP,WPxRP,RP,EP,ULTIMATELOSS");
                foreach (LayerActualMetrics l in retroLayerActualITDMetrics)
                {
                    sw.WriteLine($"{calculationId},{l.MasterKey},{l.MasterKeyFrom},{l.LayerId},{l.SubmissionId},{l.IsMultiYear},{l.IsCancellable},{l.UWYear},{l.AcctGPeriod},{l.Segment},{l.PerspectiveType.ToString()},{l.Currency},{l.Facility},{l.WrittenPremium},{l.WrittenPremiumxReinstatementPremium},{l.ReinstatementPremium},{l.EarnedPremium},{l.UltimateLoss}");
                }

                sw.Flush();
            }
        }

        private static DateTime GetQuarterEndDate(in DateTime asAtDate)
            => new DateTime((asAtDate.Month - 1) / 3 == 0 ? (asAtDate.Year - 1) : asAtDate.Year, (asAtDate.Month - 1) / 3 == 0 ? 12 : (asAtDate.Month - 1) / 3 * 3, 1).AddMonths(1).AddDays(-1);

        private static int GetAcctGPeriod(in DateTime asAtDate) 
            => (asAtDate.Month - 1) / 3 == 0 ? (asAtDate.Year - 1) * 100 + 12 : asAtDate.Year * 100 + (asAtDate.Month - 1) / 3 * 3;
    }
}
