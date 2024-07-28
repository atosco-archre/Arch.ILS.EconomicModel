
using System.Buffers;
using System.Collections.Concurrent;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

using Studio.Core;
using Studio.Core.Sql;
using Arch.ILS.Snowflake;

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

        public Task<IEnumerable<PortLayerCession>> GetPortfolioLayerCessionsParallel(int partitionCount = 8)
        {
            return Task.Factory.StartNew(() =>
            {
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
            });
        }

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
