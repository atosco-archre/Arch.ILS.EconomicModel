
using System.Buffers;
using System.Collections.Concurrent;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Studio.Core;
using Studio.Core.Sql;

namespace Arch.ILS.EconomicModel
{
    public class RevoSnowflakeRepository : SnowflakeRepository, IRevoRepository
    {
        #region Constants

        private const string GET_LAYERS = @"SELECT [LayerId]
     , [Inception]
  FROM [dbo].[Layer]";

        private const string GET_PORTFOLIOS = @"SELECT [PortfolioId]
     , [PortfolioType]
     , [AsOfDate]
  FROM [dbo].[Portfolio]";

        private const string GET_PORTFOLIO_LAYERS = @"SELECT [PortLayerId]
     , [LayerId]
     , [PortfolioId]
  FROM [dbo].[PortLayer]";

        private const string GET_RETRO_PROGRAM = @"SELECT [RetroProgramId]
     , [Inception]
     , [Expiration]
     , [RetroProgramType]
  FROM [dbo].[RetroProgram]
 WHERE [Status] IN (22,10)/*remove projection retros*/
   AND IsActive = 1
   AND IsDeleted = 0";

        private const string GET_PORTFOLIO_LAYER_CESSIONS = @"SELECT [PortLayerCessionId]
     , [PortLayerId]
     , [RetroProgramId]
     , [CessionGross]
     , [CessionNet]
  FROM [dbo].[PortLayerCession]
 WHERE IsActive = 1
   AND ShouldCessionApply = 1
   AND IsDeleted = 0
   AND CessionGross > 0";

        private const string GET_PORTFOLIO_LAYER_CESSIONS_BY_PARTITION = @"SELECT [PortLayerCessionId]
     , [PortLayerId]
     , [RetroProgramId]
     , [CessionGross]
     , [CessionNet]
  FROM [dbo].[PortLayerCession]
 WHERE (PortLayerCessionId % {1}) = {0} 
   AND IsActive = 1
   AND ShouldCessionApply = 1
   AND IsDeleted = 0
   AND CessionGross > 0";

        #endregion Constants

        public RevoSnowflakeRepository(string connectionString) : base(connectionString)
        {
        }

        public Task<Dictionary<int, Layer>> GetLayers()
        {
            return Task.Factory.StartNew(() =>
            {
                return ExecuteReaderSql(GET_LAYERS).GetObjects<Layer>().ToDictionary(x => x.LayerId);
            });
        }

        public Task<Dictionary<int, RetroProgram>> GetRetroPrograms()
        {
            return Task.Factory.StartNew(() =>
            {
                return ExecuteReaderSql(GET_RETRO_PROGRAM).GetObjects<RetroProgram>().ToDictionary(x => x.RetroProgramId);
            });
        }

        public Task<Dictionary<int, Portfolio>> GetPortfolios()
        {
            return Task.Factory.StartNew(() =>
            {
                return ExecuteReaderSql(GET_PORTFOLIOS).GetObjects<Portfolio>().ToDictionary(x => x.PortfolioId);
            });
        }

        public Task<Dictionary<int, PortLayer>> GetPortfolioLayers()
        {
            return Task.Factory.StartNew(() =>
            {
                return ExecuteReaderSql(GET_PORTFOLIO_LAYERS).GetObjects<PortLayer>().ToDictionary(x => x.PortLayerId);
            });
        }

        public Task<IEnumerable<PortLayerCession>> GetPortfolioLayerCessions()
        {
            Console.Write("0 - Non Parallel ");
            return Task.Factory.StartNew<IEnumerable<PortLayerCession>>(() =>
            {
                return ExecuteReaderSql(GET_PORTFOLIO_LAYER_CESSIONS).GetObjects<PortLayerCession>().ToArray();
            });
        }

        public Task<IEnumerable<PortLayerCession>> GetPortfolioLayerCessionsFast(int partitionCount = 8)
        {
            Console.Write("1 - Parallel For - Storage in common concurrent bag ");
            return Task.Factory.StartNew<IEnumerable<PortLayerCession>>(() =>
            {
                ConcurrentBag<PortLayerCession> portLayerCessions = new ConcurrentBag<PortLayerCession>();
                Parallel.For(0, partitionCount, i =>
                {
                    foreach (var portLayerCession in ExecuteReaderSql(string.Format(GET_PORTFOLIO_LAYER_CESSIONS_BY_PARTITION, i, partitionCount)).GetObjects<PortLayerCession>())
                        portLayerCessions.Add(portLayerCession);
                });
                return portLayerCessions;
            });
        }

        public Task<IEnumerable<PortLayerCession>> GetPortfolioLayerCessionsFast2(int partitionCount = 8)
        {
            Console.Write("2 - Parallel For - Storage in Blocking Collection Array using AddToAny and then SelectMany into same IEnumerable output ");
            return Task.Factory.StartNew<IEnumerable<PortLayerCession>>(() =>
            {
                BlockingCollection<PortLayerCession>[] portLayerCessions = new BlockingCollection<PortLayerCession>[partitionCount << 1];
                for (int i = 0; i < portLayerCessions.Length; i++)
                    portLayerCessions[i] = new BlockingCollection<PortLayerCession>();
                Parallel.For(0, partitionCount, i =>
                {
                    foreach (var portLayerCession in ExecuteReaderSql(string.Format(GET_PORTFOLIO_LAYER_CESSIONS_BY_PARTITION, i, partitionCount)).GetObjects<PortLayerCession>())
                        BlockingCollection<PortLayerCession>.AddToAny(portLayerCessions, portLayerCession);
                });
                return portLayerCessions.SelectMany(x => x);
            });
        }

        public Task<IEnumerable<PortLayerCession>> GetPortfolioLayerCessionsFast3(int partitionCount = 8)
        {
            Console.Write("3 - Parallel Task - First ExecuteReader and then Continue With Storage in common Concurrent bag passed to the cache of each Task ");
            return Task.Factory.StartNew<IEnumerable<PortLayerCession>>(() =>
            {
                ConcurrentBag<PortLayerCession> portLayerCessions = new ConcurrentBag<PortLayerCession>();
                Task[] portLayerCessionsTasks = new Task[partitionCount];
                for (int i = 0; i < portLayerCessionsTasks.Length; i++)
                    portLayerCessionsTasks[i] = Task.Factory.StartNew((index) =>
                    {
                        return ExecuteReaderSql(string.Format(GET_PORTFOLIO_LAYER_CESSIONS_BY_PARTITION, index, partitionCount)).GetObjects<PortLayerCession>();
                    }, i).ContinueWith((res, state) =>
                    {
                        ConcurrentBag<PortLayerCession> storage = (ConcurrentBag<PortLayerCession>)state!;
                        foreach (var portLayerCession in res.Result)
                            storage.Add(portLayerCession);
                    }, portLayerCessions);
                Task.WaitAll(portLayerCessionsTasks);
                return portLayerCessions;
            });
        }

        public Task<IEnumerable<PortLayerCession>> GetPortfolioLayerCessionsFast4(int partitionCount = 8)
        {
            Console.Write("4 - Parallel Task - Storage in common Concurrent bag passed to the cache of each Task ");
            return Task.Factory.StartNew<IEnumerable<PortLayerCession>>(() =>
            {
                ConcurrentBag<PortLayerCession> portLayerCessions = new ConcurrentBag<PortLayerCession>();
                Task[] portLayerCessionsTasks = new Task[partitionCount];
                for (int i = 0; i < portLayerCessionsTasks.Length; i++)
                    portLayerCessionsTasks[i] = Task.Factory.StartNew(state =>
                    {
                        Tuple<int, ConcurrentBag<PortLayerCession>> parameters = (Tuple<int, ConcurrentBag<PortLayerCession>>)state!;
                        int index = parameters!.Item1;
                        ConcurrentBag<PortLayerCession> storage = parameters.Item2;
                        foreach (var portLayerCession in ExecuteReaderSql(string.Format(GET_PORTFOLIO_LAYER_CESSIONS_BY_PARTITION, index, partitionCount)).GetObjects<PortLayerCession>())
                            storage.Add(portLayerCession);
                    }, new Tuple<int, ConcurrentBag<PortLayerCession>>(i, portLayerCessions));
                Task.WaitAll(portLayerCessionsTasks);
                return portLayerCessions;
            });
        }

        public Task<IEnumerable<PortLayerCession>> GetPortfolioLayerCessionsFast5(int partitionCount = 8)
        {
            Console.Write("5 - Parallel Task - Storage in individual list for each task and then SelectMany into one IEnumerable ");
            return Task.Factory.StartNew<IEnumerable<PortLayerCession>>(() =>
            {
                List<PortLayerCession>[] portLayerCessions = new List<PortLayerCession>[partitionCount];
                Task[] portLayerCessionsTasks = new Task[partitionCount];
                for (int i = 0; i < portLayerCessionsTasks.Length; i++)
                    portLayerCessionsTasks[i] = Task.Factory.StartNew(state =>
                    {
                        int index = (int)state!;
                        portLayerCessions[index] = ExecuteReaderSql(string.Format(GET_PORTFOLIO_LAYER_CESSIONS_BY_PARTITION, index, partitionCount)).GetObjects<PortLayerCession>().ToList();
                    }, i);
                Task.WaitAll(portLayerCessionsTasks);
                return portLayerCessions.SelectMany(x => x);
            });
        }

        public IEnumerable<PortLayerCession> GetPortfolioLayerCessionsFast6(int partitionCount = 8)
        {
            Console.Write("5 - Parallel Task - Storage in individual list for each task and then SelectMany into one IEnumerable ");
            PortLayerCession[][] portLayerCessions = new PortLayerCession[partitionCount][];
            Task[] portLayerCessionsTasks = new Task[partitionCount];
            for (int i = 0; i < portLayerCessionsTasks.Length; i++)
                portLayerCessionsTasks[i] = Task.Factory.StartNew(state =>
                {
                    int index = (int)state!;
                    portLayerCessions[index] = ExecuteReaderSql(string.Format(GET_PORTFOLIO_LAYER_CESSIONS_BY_PARTITION, index, partitionCount)).GetObjects<PortLayerCession>().ToArray();
                }, i);
            Task.WaitAll(portLayerCessionsTasks);
            return portLayerCessions.SelectMany(x => x);
        }


        public void GetPortfolioLayerCessionsNoStorage()
        {
            Console.Write("0 - Non Parallel ");
            foreach (var portLayerCession in ExecuteReaderSql(GET_PORTFOLIO_LAYER_CESSIONS).GetObjects<PortLayerCession>())
            {
            }
        }

        public void GetPortfolioLayerCessionsNoStorageParallel(int partitionCount = 8)
        {
            Console.Write("1 - Parallel For");
            Parallel.For(0, partitionCount, i =>
            {
                foreach (var portLayerCession in ExecuteReaderSql(string.Format(GET_PORTFOLIO_LAYER_CESSIONS_BY_PARTITION, i, partitionCount)).GetObjects<PortLayerCession>())
                {
                }
            });
        }

        public void GetPortfolioLayerCessionsNoStorageTasks(int partitionCount = 8)
        {
            Console.Write("5 - Parallel Task");
            Task[] portLayerCessionsTasks = new Task[partitionCount];
            for (int i = 0; i < portLayerCessionsTasks.Length; i++)
                portLayerCessionsTasks[i] = Task.Factory.StartNew(state =>
                {
                    int index = (int)state!;
                    foreach(var portLayerCession in ExecuteReaderSql(string.Format(GET_PORTFOLIO_LAYER_CESSIONS_BY_PARTITION, index, partitionCount)).GetObjects<PortLayerCession>())
                    {
                    }
                }, i);
            Task.WaitAll(portLayerCessionsTasks);
        }

        //public void GetLayerView(int partitionCount = 8)
        //{
        //    const int ROWSIZE = 45;
        //    Task[] portLayerCessionsTasks = new Task[partitionCount];
        //    for (int i = 0; i < portLayerCessionsTasks.Length; i++)
        //        portLayerCessionsTasks[i] = Task.Factory.StartNew(state =>
        //        {
        //            int index = (int)state!;
        //            byte[] buffer = ArrayPool<byte>.Shared.Rent(ROWSIZE);
        //            ref byte bufferStart = ref MemoryMarshal.GetArrayDataReference(buffer);
        //            SqlDataReader reader = (SqlDataReader)ExecuteReaderSql(string.Format(GET_PORTFOLIO_LAYER_CESSIONS_BY_PARTITION, index, partitionCount), CommandBehavior.SequentialAccess);
        //            if(reader.Read()) 
        //            {
        //                using (Stream stream = reader.GetStream(0))
        //                {
        //                    while(stream.Read(buffer, 0, ROWSIZE) == ROWSIZE)
        //                    {
        //                        PortLayerCessionRefStruct portLayerCession = new PortLayerCessionRefStruct(ref bufferStart);
        //                        Console.WriteLine(portLayerCession.CessionNet);
        //                    }
        //                }
        //            }
        //        }, i);
        //    Task.WaitAll(portLayerCessionsTasks);
        //}

        public void GetLayerView(int partitionCount = 8)
        {
            var portLayersTask = GetPortfolioLayers();
            var layersTask = GetLayers();
            var portfoliosTask = GetPortfolios();
            var retroProgramsTask = GetRetroPrograms();
            Task.WaitAll(portLayersTask, layersTask, portfoliosTask, retroProgramsTask);
            Dictionary<int, PortLayer> portLayers = portLayersTask.Result;
            Dictionary<int, Layer> layers = layersTask.Result;
            Dictionary<int, Portfolio> portfolios = portfoliosTask.Result;
            Dictionary<int, RetroProgram> retroPrograms = retroProgramsTask.Result;

            Task[] portLayerCessionsTasks = new Task[partitionCount];
            for (int i = 0; i < portLayerCessionsTasks.Length; i++)
                portLayerCessionsTasks[i] = Task.Factory.StartNew(state =>
                {
                    int index = (int)state!;
                    
                    foreach(var portLayerCession in ExecuteReaderSql(string.Format(GET_PORTFOLIO_LAYER_CESSIONS_BY_PARTITION, index, partitionCount)).GetObjects<PortLayerCession>())
                    {
                        //if (!retroPrograms.TryGetValue(portLayerCession.RetroProgramId, out RetroProgram retroProgram))
                        //    continue;

                        //PortLayer portLayer = portLayers[portLayerCession.PortLayerId];
                        //Portfolio portfolio = portfolios[portLayer.PortfolioId];
                        //Layer layer = layers[portLayer.LayerId];
                        //DateTime? inception;
                        //if ((inception = GetPortfolioLayerInception(portfolio, layer)) == null)
                        //    continue;
                        //DateTime portLayerInception = (DateTime)inception;
                        //DateTime portLayerExpiration = portLayerInception.AddYears(1).AddDays(-1);

                        //if (portLayerExpiration < retroProgram.Inception 
                        //    || portLayerInception > retroProgram.Expiration
                        //    || (retroProgram.RetroProgramType != 1 /*1 = LOD*/ && portLayerInception < retroProgram.Inception))/*if RAD, discard ones where the layer started before the retro*/
                        //    continue;

                        //DateTime overlapStart = retroProgram.RetroProgramType != 2 /*2 = RAD*/ && retroProgram.Inception > portLayerInception 
                        //    ? retroProgram.Inception
                        //    : portLayerInception;
                        //DateTime overlapEnd = retroProgram.RetroProgramType != 2 /*2 = RAD*/ && retroProgram.Expiration < portLayerExpiration
                        //    ? retroProgram.Expiration
                        //    : portLayerExpiration;
                    }
                }, i);
            Task.WaitAll(portLayerCessionsTasks);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static DateTime? GetPortfolioLayerInception(Portfolio portfolio, Layer layer)
            {
                return portfolio.PortfolioType switch
                {
                    0 => layer.Inception,
                    1 => layer.Inception.AddYears(1),
                    _ when portfolio.PortfolioType == 2 && layer.Inception.Year == portfolio.AsOfDate.Year => layer.Inception,
                    _ when portfolio.PortfolioType == 2 && layer.Inception.Year == portfolio.AsOfDate.Year - 1 => layer.Inception.AddYears(1),
                    _ when portfolio.PortfolioType == 3 && layer.Inception.Year == portfolio.AsOfDate.Year => layer.Inception.AddYears(1),
                    _ when portfolio.PortfolioType == 3 && layer.Inception.Year == portfolio.AsOfDate.Year - 1 => layer.Inception.AddYears(2),
                    _ => null
                };
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Range[] GetYeltDayRanges(in DateTime inceptionDate, in DateTime expirationDate)
        {
            const int firstDayOfYear = 1;
            const int lastDayOfYear = 365;
            int days = (expirationDate - inceptionDate).Days + 1;
            if (days < 0)
                throw new ArgumentException("Expected an Expiration Date >= Inception Date");
            if (days >= lastDayOfYear)
                return [new Range(firstDayOfYear, lastDayOfYear)];
            if (inceptionDate.DayOfYear > expirationDate.DayOfYear)  //period intersecting two successive calendar years
                return [new Range(firstDayOfYear, expirationDate.DayOfYear), new Range(inceptionDate.DayOfYear, lastDayOfYear)];
            else return [new Range(inceptionDate.DayOfYear, expirationDate.DayOfYear)];
        }
    }
}
