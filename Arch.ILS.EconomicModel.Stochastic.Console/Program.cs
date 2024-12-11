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
            simulationFactory.GetConditionYelt(false, 1, "Retro 251 - Actual ITD 202409 - As At 2024-12-10 - Archview", 251, DateTime.Today, [RevoLossViewType.ArchView]);
        }
    }
}