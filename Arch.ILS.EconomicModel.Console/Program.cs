
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Arch.ILS.Common;
using Arch.ILS.EconomicModel;
using Google.Apis.Storage.v1.Data;

namespace Arch.ILS.EconomicModel.Console
{
    public class Program
    {
        public static unsafe void Main(string[] args)
        {
            int bucket = 0;
            nuint i = (uint)bucket - 1;
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
            //var portfolios = revoRepository.GetPortfolios().Result.ToDictionary(x => x.PortfolioId);
            //var portLayers = revoRepository.GetPortfolioLayers().Result.ToDictionary(x => x.PortLayerId);
            //var result = revoRepository.GetLayerView();
            //var x = result.GetPortfolioLevelLayerCessions().Where(x => x.RetroProgramId == 218).ToArray();
            //var xd = result.GetPortfolioLevelLayerCessions().Where(x => x.RetroProgramId == 218 && x.PortLayerId == 750703).ToArray();
            //var portLayersCessions2 = revoRepository.GetPortfolioLayerCessionsParallel().Result.ToArray();

            RevoLayerLossSqlRepository revoLayerLossSqlRepository = new RevoLayerLossSqlRepository(connectionSettings.GetConnectionString(RevoConnectionStrings.REVOLAYERLOSS));
            var layerYelt = revoLayerLossSqlRepository.GetLayerDayYeltVectorised(10619, 38252).Result;
            YeltPartitioner yeltPartitioner = new YeltPartitioner(new Range[] { new Range(2, 50) }, layerYelt);
            YeltPartitionReader yeltPartitionLinkedListReader = YeltPartitionReader.Initialise(yeltPartitioner);
            var layerYelt2 = revoLayerLossSqlRepository.GetLayerDayYeltVectorised(10620, 38252).Result;
            YeltPartitioner yeltPartitioner2 = new YeltPartitioner(new Range[] { new Range(2, 50) }, layerYelt2);
            YeltPartitionReader yeltPartitionLinkedListReader2 = YeltPartitionReader.Initialise(yeltPartitioner2);

            long[] sortedKeys = new long[yeltPartitionLinkedListReader.TotalLength + yeltPartitionLinkedListReader2.TotalLength];
            fixed(long*keysPtr = sortedKeys)
            {
                long* ptr = keysPtr;
                int keyCount = YeltPartitionMerge.Merge_ScalarOptimised_2(yeltPartitionLinkedListReader.Head, yeltPartitionLinkedListReader2.Head, ptr);
                Array.Resize(ref sortedKeys, keyCount);
            }
            YeltPartitionMapper mapper = new YeltPartitionMapper(new[] { yeltPartitionLinkedListReader, yeltPartitionLinkedListReader2 }, sortedKeys);

            int j = 0;
            Stopwatch sw = Stopwatch.StartNew();
            int[] rows = null;
            while (j++ <= 100)
            {
                rows = mapper.MapKeys();
                mapper.Reset();
            }
            sw.Stop();
            System.Console.WriteLine(sw);
            
            System.Console.ReadLine();
        }
    }
}

