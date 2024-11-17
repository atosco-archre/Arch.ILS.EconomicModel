
using Arch.ILS.Common;
using LargeLoss.LayeringService.Client;
using System.Collections.Concurrent;

namespace Arch.ILS.EconomicModel.Historical.Console
{
    public class Program
    {
        public static void Main(string[] args)
        {
            //ExportReinstatements();
            //ExportRetroCessions("Retro Allocations Revo Bermuda As Of 2024-10-21");
            DateTime scenarioStartPeriod = new DateTime(2010, 1, 1, 0, 0, 0, DateTimeKind.Utc);//20100101
            DateTime scenarioEndPeriod = new DateTime(2024, 9, 6, 0, 0, 0, DateTimeKind.Utc);//20240906
            GetScenarioLayerLossAggregates(scenarioStartPeriod, scenarioEndPeriod, "All Scenarios ARL-ARE-ASL-WATFORD 2010-01-01 to 2024-09-06");
        }

        public static void GetScenarioLayerLossAggregates(DateTime scenarioStartPeriod, DateTime scenarioEndPeriod, string headerName)
        {
            ScenarioConnectionStrings scenarioConnectionSettings = new ScenarioConnectionStrings();
            ScenarioSqlRepository scenarioRepository = new ScenarioSqlRepository(scenarioConnectionSettings.GetConnectionString(ScenarioConnectionStrings.ARG_DWVARLSQL01));
            Dictionary<long, Scenario> scenarios = scenarioRepository.GetScenarios().Result
                .Where(x => x.IsActive && !x.IsDeleted)
                .ToDictionary(x => x.ScenarioId);
            Dictionary<long, ScenarioLossEvent> scenarioLossEvents = scenarioRepository.GetScenarioLossEvents().Result
                .Where(x => x.IsActive && !x.IsDeleted && scenarioStartPeriod <= x.EventDate && x.EventDate <= scenarioEndPeriod)
                .ToDictionary(x => x.ScenarioId);

            //var allscenarioLayersTask = scenarioRepository.GetScenarioLayers();
            //var allscenarioLayerLossesTask = scenarioRepository.GetScenarioLayerLosses();
            //Task.WaitAll(allscenarioLayersTask, allscenarioLayerLossesTask);
            //var allscenarioLayers = allscenarioLayersTask.Result.ToList();
            //var allscenarioLayerLosses = allscenarioLayerLossesTask.Result.ToList();

            HashSet<long> layerIds = new HashSet<long>();
            ConcurrentDictionary<long, Dictionary<int, (ScenarioLayer, ScenarioLayerLoss)>> scenariosLayerLosses = new ConcurrentDictionary<long, Dictionary<int, (ScenarioLayer, ScenarioLayerLoss)>>();
            //foreach (long scenarioId in scenarioLossEvents.Keys)
            Parallel.ForEach(scenarioLossEvents.Keys, (scenarioId) =>
            {
                Dictionary<int, ScenarioLayer> scenarioLayers = scenarioRepository.GetScenarioLayers(scenarioId).Result.Where(x => x.IsActive && !x.IsDeleted).ToDictionary(x => x.LayerId);
                Dictionary<int, ScenarioLayerLoss> scenarioLayerLosses = scenarioRepository.GetScenarioLayerLosses(scenarioId).Result.ToDictionary(x => x.LayerId);
                var scenariolayersLosses = new Dictionary<int, (ScenarioLayer, ScenarioLayerLoss)>();
                scenariosLayerLosses[scenarioId] = scenariolayersLosses;
                foreach (int layerId in scenarioLayers.Keys)
                {
                    layerIds.Add(layerId);
                    scenariolayersLosses.Add(layerId, (scenarioLayers[layerId], scenarioLayerLosses[layerId]));
                }
            });

            IEnumerable<ScenarioLayerSection> dbSections = scenarioRepository.GetScenarioLayerSections().Result.Where(x => scenarioLossEvents.ContainsKey(x.ScenarioId));
            Dictionary<int, DtoReinstatement[]> layerReinstatements = scenarioRepository.GetScenarioLayerReinstatements().Result
                .GroupBy(x => (int)x.LayerId).ToDictionary(k => k.Key, v => v.Select(s => new DtoReinstatement { ReinstatementId = s.ReinstatementId, ReinstatementOrder = s.ReinstatementOrder, Quantity = s.Quantity, PremiumShare = s.PremiumShare, BrokeragePercentage = s.BrokeragePercentage }).Distinct().OrderBy(o => o.ReinstatementOrder).ToArray());
            DtoLayeringRequest layeringRequest = new DtoLayeringRequest { ApplyLossAggregation = true, LayerEvents = new List<DtoLayeringInput>(), Layers = new List<DtoLayerInput>(), Events = new List<DtoEventInput>(), Sections = GetLayeringSection(dbSections).ToList() };
            LayeringServiceClient layeringClient = new LayeringServiceClient("https://localhost:44397/"/*"https://apps-dev.archre.com/actuarial/largeloss/api/layering/service/"*/, new HttpClient());//https://apps-dev.archre.com/actuarial/largeloss/api/layering/service/swagger/index.html
            Dictionary<int, DtoLayerInput> distinctLayers = new Dictionary<int, DtoLayerInput>();
            Dictionary<string, DtoEventInput> distinctEvents = new Dictionary<string, DtoEventInput>();
            foreach (long scenarioId in scenarioLossEvents.Keys)
            {
                if (!scenariosLayerLosses.TryGetValue(scenarioId, out var layerLossesByLayerId))
                    continue;

                foreach (var layerIdLosses in layerLossesByLayerId)
                {
                    int layerId = layerIdLosses.Key;
                    ScenarioLayer scenarioLayer = layerIdLosses.Value.Item1;
                    ScenarioLayerLoss scenarioLayerLoss = layerIdLosses.Value.Item2;
                    ScenarioLossEvent scenarioLossEvent = scenarioLossEvents[scenarioId];
                    layerReinstatements.TryGetValue(layerId, out DtoReinstatement[] reinstatemements);
                    var layeringInputs = GetLayeringRequest(scenarioLossEvent.EventDate, scenarioLayer, scenarioLayerLoss, reinstatemements);
                    layeringRequest.LayerEvents.Add(layeringInputs.layeringInput);
                    if(distinctLayers.TryAdd(layeringInputs.layerInput.LayerId, layeringInputs.layerInput))
                        layeringRequest.Layers.Add(layeringInputs.layerInput);
                    if (distinctEvents.TryAdd(layeringInputs.eventInput.EventId, layeringInputs.eventInput))
                        layeringRequest.Events.Add(layeringInputs.eventInput);
                }
            }

            var layeringResponse = layeringClient.ComputePartitionedLossesAsync(layeringRequest).Result;
            scenarioRepository.Save(new ScenarioLayerLossAggregateHeader(headerName, scenarioStartPeriod, scenarioEndPeriod, DateTime.Now), layeringResponse.LayerEvents);
            //ScenarioRetroCession[] scenarioRetroCessions = scenarioRepository.GetScenarioRetroCessions().Result.ToArray();
            //ScenarioRetroCessionLoss[] scenarioRetroCessionLosses = scenarioRepository.GetScenarioRetroCessionLosses().Result.ToArray();
        }

        public static void ExportRetroCessions(string exportName)
        {
            /*Authentication*/
            //ConnectionProtection connectionProtection =
            //    new ConnectionProtection(@"C:\Users\atosco\source\repos\Arch.ILS.EconomicModel\Arch.ILS.EconomicModel.Console\App.config.config");
            //RevoConnectionStrings connectionSettings = new RevoConnectionStrings(connectionProtection, false);
            //RevoRepository revoRepository = new RevoSqlRepository(connectionSettings.GetConnectionString(RevoConnectionStrings.REVO));
            RevoRepository revoRepository = new RevoSnowflakeRepository(new SnowflakeConnectionStrings().RevoBermudaConnectionString);
            var retroAllocationView = revoRepository.GetRetroAllocationView().Result;
            var levelLayerCessions = retroAllocationView.GetLevelLayerCessions();
            ScenarioConnectionStrings scenarioConnectionSettings = new ScenarioConnectionStrings();
            ScenarioSqlRepository scenarioRepository = new ScenarioSqlRepository(scenarioConnectionSettings.GetConnectionString(ScenarioConnectionStrings.ARG_DWVARLSQL01));
            scenarioRepository.Save(new LayerPeriodCessionHeader(exportName, DateTime.Now), levelLayerCessions);
        }

        public static void ExportReinstatements()
        {
            /*Authentication*/
            //ConnectionProtection connectionProtection =
            //    new ConnectionProtection(@"C:\Users\atosco\source\repos\Arch.ILS.EconomicModel\Arch.ILS.EconomicModel.Console\App.config.config");
            //RevoConnectionStrings connectionSettings = new RevoConnectionStrings(connectionProtection, false);
            //RevoRepository revoRepository = new RevoSqlRepository(connectionSettings.GetConnectionString(RevoConnectionStrings.REVO));
            RevoRepository revoRepository = new RevoSnowflakeRepository(new SnowflakeConnectionStrings().RevoBermudaConnectionString);
            var layerReinstatements = revoRepository.GetLayerReinstatements().Result;
            ScenarioConnectionStrings scenarioConnectionSettings = new ScenarioConnectionStrings();
            ScenarioSqlRepository scenarioRepository = new ScenarioSqlRepository(scenarioConnectionSettings.GetConnectionString(ScenarioConnectionStrings.ARG_DWVARLSQL01));
            var allScenarioLayersTask = scenarioRepository.GetScenarioLayers().Result;
            var scenarioLayerReinstatements = allScenarioLayersTask.Join(layerReinstatements, i => i.LayerId, o => o.LayerId, (i, o) => new ScenarioLayerReinstatement(i.ScenarioId, i.LayerId, o.ReinstatementId, o.ReinstatementOrder, o.Quantity, o.PremiumShare, o.BrokeragePercentage, BitConverter.IsLittleEndian ? BitConverter.GetBytes(o.RowVersion).Reverse().ToArray() : BitConverter.GetBytes(o.RowVersion)));
            scenarioRepository.Save(scenarioLayerReinstatements);
        }

        public static (DtoLayeringInput layeringInput, DtoLayerInput layerInput, DtoEventInput eventInput) GetLayeringRequest(DateTime eventDate, ScenarioLayer layer, ScenarioLayerLoss layerLoss, DtoReinstatement[] reinstatements)
        {
            if (layerLoss.LossCurrency != "USD")
                throw new NotImplementedException("Expected the loss currency to be USD");
            DtoLayeringInput layeringInput = new DtoLayeringInput
            {
                EventId = layerLoss.ScenarioId.ToString(),//largeLoss.EventKey
                LayerId = layer.LayerId,
                GuLossInLayerCurrency = layerLoss.GULoss ?? 0m,//Layer loss values are in loss currency while layer values are in layer currency 
                Lae = layerLoss.LAE,
            };

            decimal occLimit = GetOccLimit(layer);
            DtoLayerInput layerInput = new DtoLayerInput
            {
                InceptionDate = layer.Inception.ToUniversalTime().Date,
                ExpirationDate = layer.Expiration.ToUniversalTime().Date,
                LayerId = layer.LayerId,
                OccLimit = occLimit / layerLoss.FXRateToLayerCurrency,
                OccRetention = GetOccRetention(layer) / layerLoss.FXRateToLayerCurrency,
                AggLimit = layer.AggLimit / layerLoss.FXRateToLayerCurrency,
                AggRetention = layer.AggRetention / layerLoss.FXRateToLayerCurrency,
                Franchise = layer.Franchise / layerLoss.FXRateToLayerCurrency,
                FranchiseReverse = layer.FranchiseReverse / layerLoss.FXRateToLayerCurrency,
                Placement = layer.Placement,
                IsFHCF = layerLoss.IsFHCF,
                Currency = layerLoss.LossCurrency,
                Premium = layer.Premium,
                Reinstatements = (occLimit == decimal.Zero || reinstatements == null || reinstatements.Sum(x => (double)(x.Quantity ?? 0.0)) == 0.0) ? null : reinstatements,
            };

            DtoEventInput eventInput = new DtoEventInput
            {
                EventId = layerLoss.ScenarioId.ToString(),//largeLoss.EventKey
                EventDate = eventDate.ToUniversalTime().Date,
            };

            return (layeringInput, layerInput, eventInput);
        }

        public static IEnumerable<DtoLayeringSection> GetLayeringSection(IEnumerable<ScenarioLayerSection> scenarioLayerSections)
        {
            return scenarioLayerSections.Select(x => new DtoLayeringSection { LayerId = (int)x.LayerId, SectionId = (int)x.SectionId, RollUpType = (ERollUpType)x.RollUpType, FxRateToParent = x.FXRateToParent })
                .Distinct();//DtoLayeringSection is a record class
        }

        private static decimal GetOccLimit(ScenarioLayer scenarioLayer)
        {
            return scenarioLayer.LimitBasis switch
            {
                1 => scenarioLayer.AggLimit,
                4 or 7 => scenarioLayer.RiskLimit,
                _ => scenarioLayer.OccLimit
            };
        }

        private static decimal GetOccRetention(ScenarioLayer scenarioLayer)
        {
            return scenarioLayer.LimitBasis switch
            {
                1 => scenarioLayer.AggRetention,
                4 or 7 => scenarioLayer.RiskRetention,
                _ => scenarioLayer.OccRetention
            };
        }
    }
}

