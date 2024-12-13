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
            ConditionalCalculationInput inputWithErosion = new ConditionalCalculationInput
            {
                CalculationName = "Retro 251 - Actual ITD 202409 - As At 2024-12-10 - Archview",
                RetroProgramId = 251,
                ApplyErosion = true,
                LossViews = [RevoLossViewType.ArchView],
                ResetType = ResetType.RAD,
                UseBoundFx = true,
                BaseCurrency = Currency.USD,
                CurrentFXDate = new DateTime(2024, 9, 30),
                ConditionalCutoffDate = new DateTime(2024, 12, 1),
                AsAtDate = DateTime.Now,
                NonGULossBasedLayers = new HashSet<int>
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
                }
            };            
            /*Process*/
            SimulationFactory simulationFactory = new SimulationFactory(revoRepository, revoLayerLossRepository, revoGULossRepository, actuarialStochasticRepository, mixedRepository);
            simulationFactory.InitialiseCalculationExport(inputWithErosion);//Sets the CalculationId
            ConditionalCalculationInput inputExcludingErosion = inputWithErosion with { ApplyErosion = false };
            IList<LayerActualMetrics> layerActualMetrics = simulationFactory.ExportLayerActualITDMetrics(inputWithErosion, LayerActualMetrics.LoadFromCsv(@"C:\Data\LayerActualITDMetrics_20241211.csv").ToList());
            simulationFactory.ExportYelt(inputWithErosion, !inputWithErosion.ApplyErosion, layerActualMetrics);
            simulationFactory.ExportYelt(inputExcludingErosion, !inputWithErosion.ApplyErosion, layerActualMetrics);
            simulationFactory.ExportRetroLayerCessions(inputWithErosion);
        }
    }
}