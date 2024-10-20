
using Arch.ILS.Common;
using LargeLoss.LayeringService.Client;
using System.Collections.Concurrent;

namespace Arch.ILS.EconomicModel.Historical.Console
{
    public class Program
    {
        public static void Main(string[] args)
        {
            DateTime scenarioStartPeriod = new DateTime(2020, 1, 1);
            DateTime scenarioEndPeriod = new DateTime(2022, 10, 19);
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
            DtoLayeringRequest layeringRequest = new DtoLayeringRequest { ApplyLossAggregation = true, Layers = new List<DtoLayeringInput>(), Sections = GetLayeringSection(dbSections).ToList() };
            LayeringServiceClient layeringClient = new LayeringServiceClient("https://localhost:44397/"/*"https://apps-dev.archre.com/actuarial/largeloss/api/layering/service/"*/, new HttpClient());//https://apps-dev.archre.com/actuarial/largeloss/api/layering/service/swagger/index.html

            foreach (long scenarioId in scenarioLossEvents.Keys) 
            {
                if (!scenariosLayerLosses.TryGetValue(scenarioId, out var layerLossesByLayerId))
                    continue;

                foreach(var layerIdLosses in layerLossesByLayerId)
                {
                    int layerId = layerIdLosses.Key;
                    ScenarioLayer scenarioLayer = layerIdLosses.Value.Item1;
                    ScenarioLayerLoss scenarioLayerLoss = layerIdLosses.Value.Item2;
                    ScenarioLossEvent scenarioLossEvent = scenarioLossEvents[scenarioId];
                    //if (!TryMapEventDate(scenarioLossEvent.EventDate, scenarioLayer.Inception, scenarioLayer.Expiration, out DateTime mappedEventDate))
                    //    continue;
                    DtoLayeringInput layeringInput = GetLayeringRequest(scenarioLossEvent.EventDate, scenarioLayer, scenarioLayerLoss);
                    layeringRequest.Layers.Add(layeringInput); 
                }
            }

            var layeringResponse = layeringClient.ComputePartitionedLossesAsync(layeringRequest).Result;
            ScenarioRetroCession[] scenarioRetroCessions = scenarioRepository.GetScenarioRetroCessions().Result.ToArray();
            ScenarioRetroCessionLoss[] scenarioRetroCessionLosses = scenarioRepository.GetScenarioRetroCessionLosses().Result.ToArray();
        }

        public static bool TryMapEventDate(DateTime eventDate, DateTime layerInception, DateTime layerExpiration, out DateTime mappedEventDate)
        {
            DateTime candidateEventDate = new DateTime(layerInception.Year, eventDate.Month, eventDate.Day);
            if (candidateEventDate >= layerInception && candidateEventDate <= layerExpiration)
            {
                mappedEventDate = candidateEventDate;
                return true;
            }
            candidateEventDate = new DateTime(layerInception.Year + 1, eventDate.Month, eventDate.Day);
            if (candidateEventDate >= layerInception && candidateEventDate <= layerExpiration)
            {
                mappedEventDate = candidateEventDate;
                return true;
            }
            mappedEventDate = DateTime.MinValue;
            return false;
        }

        public static DtoLayeringInput GetLayeringRequest(DateTime eventDate, ScenarioLayer layer, ScenarioLayerLoss layerLoss)
        {
            if (layerLoss.LossCurrency != "USD")
                throw new NotImplementedException("Expected the loss currency to be USD");
            return new DtoLayeringInput
            {
                EventId = layerLoss.ScenarioId.ToString(),//largeLoss.EventKey
                EventDate = eventDate,
                InceptionDate = layer.Inception,
                ExpirationDate = layer.Expiration,
                LayerId = layer.LayerId,
                GuLoss = (double)(layerLoss.GULoss ?? 0m),//Layer loss values are in loss currency while layer values are in layer currency 
                OccLimit = (double)(layer.OccLimit / layerLoss.FXRateToLayerCurrency),
                OccRetention = (double)(layer.OccRetention / layerLoss.FXRateToLayerCurrency),
                AggLimit = (double)(layer.AggLimit / layerLoss.FXRateToLayerCurrency),
                AggRetention = (double)(layer.AggRetention / layerLoss.FXRateToLayerCurrency),
                Franchise = (double)(layer.Franchise / layerLoss.FXRateToLayerCurrency),
                FranchiseReverse = (double)(layer.FranchiseReverse / layerLoss.FXRateToLayerCurrency),
                Placement = (double)layer.Placement,
                IsFHCF = layerLoss.IsFHCF,
                Lae = (double)layerLoss.LAE,
                Currency = layerLoss.LossCurrency
            };
        }

        public static IEnumerable<DtoLayeringSection> GetLayeringSection(IEnumerable<ScenarioLayerSection> scenarioLayerSections)
        {
            return scenarioLayerSections.Select(x => new DtoLayeringSection { LayerId = x.LayerId, SectionId = x.SectionId, RollUpType = (ERollUpType)x.RollUpType, FxRateToParent = (double)x.FXRateToParent })
                .Distinct();
        }
    }
}

