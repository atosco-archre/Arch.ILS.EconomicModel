
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
            RevoConnectionStrings connectionSettings = new RevoConnectionStrings(connectionProtection, false);
            RevoSqlRepository revoRepository = new RevoSqlRepository(connectionSettings.GetConnectionString(RevoConnectionStrings.REVO));


            /*Queries*/
            //var Layers = revoRepository.GetLayers().Result.ToDictionary(x => x.LayerId);
            //var portfolios = revoRepository.GetPortfolios().Result.ToDictionary(x => x.PortfolioId);
            //var portLayers = revoRepository.GetPortfolioLayers().Result.ToDictionary(x => x.PortLayerId);

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

