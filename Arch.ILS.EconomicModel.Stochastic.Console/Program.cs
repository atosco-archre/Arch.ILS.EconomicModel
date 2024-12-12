using Arch.ILS.Common;
using System.Collections.Generic;

namespace Arch.ILS.EconomicModel.Stochastic.Console
{
    public class Program
    {
        public static void Main(string[] args)
        {
            /*Repositories*/
            IRevoRepository revoRepository = new RevoSnowflakeRepository(new SnowflakeConnectionStrings().RevoBermudaConnectionString);
            IRevoLayerLossRepository revoLayerLossRepository = new RevoLayerLossSnowflakeRepository(new SnowflakeConnectionStrings().RevoLayerLossBermudaConnectionString);
            IRevoGULossRepository revoGULossRepository = new RevoGULossSnowflakeRepository(new SnowflakeConnectionStrings().RevoGULossBermudaConnectionString);
            IActuarialStochasticRepository actuarialStochasticRepository = new ActuarialStochasticSnowflakeRepository(new SnowflakeConnectionStrings().ActuarialILSPOCStcConnectionString);
            IMixedRepository mixedRepository = new MixedSnowflakeRepository(new SnowflakeConnectionStrings().RevoBermudaConnectionString);

            /*Input*/
            HashSet<int> nonGULossBasedLayers = new HashSet<int>
            {
                136866,
                138466,
                138564,
                141578,
                145415,
                145927,
                147252,
                149525,
                159513,
                159514
            };
            int calculationId = 1;
            string calculationName = "Retro 251 - Actual ITD 202409 - As At 2024-12-10 - Archview";
            int retroProgramId = 251;
            bool applyErosion = true;
            HashSet<RevoLossViewType> lossViews = [RevoLossViewType.ArchView];
            ResetType resetType = ResetType.RAD;
            bool useBoundFx = true;
            Currency baseCurrency = Currency.USD;
            DateTime currentFXDate = new DateTime(2024, 9, 30);
            DateTime conditionalDate = new DateTime(2024, 12, 1);
            DateTime asAtDate = DateTime.Now;
            /*Process*/
            SimulationFactory simulationFactory = new SimulationFactory(revoRepository, revoLayerLossRepository, revoGULossRepository, actuarialStochasticRepository, mixedRepository);
            simulationFactory.InitialiseCalculationExport(in calculationId, in calculationName, in conditionalDate, in asAtDate, in useBoundFx, baseCurrency.ToString(), in currentFXDate);
            IList<LayerActualMetrics> layerActualMetrics = LayerActualMetrics.LoadFromCsv(@"C:\Data\LayerActualITDMetrics_20241211.csv").ToList();
            IList<LayerActualMetrics> layerActualMetricsSaved = simulationFactory.ExportLayerActualITDMetrics(in applyErosion, in calculationId, in retroProgramId, asAtDate, layerActualMetrics);
            simulationFactory.ExportYelt(applyErosion, calculationId, retroProgramId, conditionalDate, asAtDate, lossViews, false, layerActualMetricsSaved, nonGULossBasedLayers);
            simulationFactory.ExportYelt(false, calculationId, retroProgramId, conditionalDate, asAtDate, lossViews, true, layerActualMetricsSaved, nonGULossBasedLayers);
            simulationFactory.ExportRetroLayerCessions(calculationId, resetType, asAtDate, currentFXDate, useBoundFx, baseCurrency);
        }
    }
}