
using System.Diagnostics;

using Arch.ILS.Common;
using Arch.ILS.EconomicModel;

namespace Arch.ILS.EconomicModel.Console
{
    public class Program
    {
        public static void Main(string[] args)
        {
            /*Authentication*/
            ConnectionProtection connectionProtection =
                new ConnectionProtection(@"C:\Users\atosco\source\repos\Arch.ILS.EconomicModel\Arch.ILS.EconomicModel.Console\App.config.config");
            //if (!connectionProtection.IsProtected())
            //    connectionProtection.EncryptFile();
            //RevoSnowflakeRepository revoSnowflakeRepository = new RevoSnowflakeRepository(new SnowflakeConnectionStrings().ConnectionString);
            //var layers = revoSnowflakeRepository.GetLayers().Result;
            //var portLayersCessions = revoSnowflakeRepository.GetPortfolioLayerCessionsParallel().Result.ToArray();
            RevoConnectionStrings connectionSettings = new RevoConnectionStrings(connectionProtection, false);
            RevoSqlRepository revoRepository = new RevoSqlRepository(connectionSettings.GetConnectionString(RevoConnectionStrings.REVO));


            /*Queries*/
            //var Layers2 = revoRepository.GetLayers().Result;
            ////var portfolios = revoRepository.GetPortfolios().Result.ToDictionary(x => x.PortfolioId);
            ////var portLayers = revoRepository.GetPortfolioLayers().Result.ToDictionary(x => x.PortLayerId);
            //var portLayersCessions2 = revoRepository.GetPortfolioLayerCessionsParallel().Result.ToArray();

            RevoLayerLossSqlRepository revoLayerLossSqlRepository = new RevoLayerLossSqlRepository(connectionSettings.GetConnectionString(RevoConnectionStrings.REVOLAYERLOSS));
            var layerYelt = revoLayerLossSqlRepository.GetLayerDayYelVectorised(10619, 38252).Result;
            YeltPartitioner yeltPartitioner = new YeltPartitioner(new Range[] {new Range(1, 50)}, layerYelt);
            yeltPartitioner.TryGetCurrentPartition(out var yeltDayPartition);
            const int partitionCount = 8;
            Stopwatch sw = Stopwatch.StartNew();
            revoRepository.GetLayerView();
            sw.Stop();
            System.Console.WriteLine(sw.Elapsed);
            //Thread.Sleep(5000);
            //sw.Restart();

            System.Console.WriteLine("End.");
        }
    }
}

