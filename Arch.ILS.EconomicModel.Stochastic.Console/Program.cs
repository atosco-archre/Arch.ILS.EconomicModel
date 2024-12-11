using Arch.ILS.Common;

namespace Arch.ILS.EconomicModel.Stochastic.Console
{
    public class Program
    {
        public static void Main(string[] args)
        {
            IRevoRepository revoRepository = new RevoSnowflakeRepository(new SnowflakeConnectionStrings().RevoBermudaConnectionString);
            IRevoLayerLossRepository revoLayerLossRepository = new RevoLayerLossSnowflakeRepository(new SnowflakeConnectionStrings().RevoLayerLossBermudaConnectionString);
            IRevoGULossRepository revoGULossRepository = new RevoGULossSnowflakeRepository(new SnowflakeConnectionStrings().RevoGULossBermudaConnectionString);
            IActuarialStochasticRepository actuarialStochasticRepository = new ActuarialStochasticSnowflakeRepository(new SnowflakeConnectionStrings().ActuarialILSPOCStcConnectionString);
            IMixedRepository mixedRepository = new MixedSnowflakeRepository(new SnowflakeConnectionStrings().RevoBermudaConnectionString);

            SimulationFactory simulationFactory = new SimulationFactory(revoRepository, revoLayerLossRepository, revoGULossRepository, actuarialStochasticRepository, mixedRepository);
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
            simulationFactory.GetConditionYelt(false, 1, "Retro 251 - Actual ITD 202409 - As At 2024-12-10 - Archview", 251, DateTime.Today, [RevoLossViewType.ArchView], nonGULossBasedLayers);
        }
    }
}