
using Arch.ILS.Common;
using BenchmarkDotNet.Attributes;
using Arch.ILS.EconomicModel;

namespace Arch.ILS.EconomicModel.Benchmark
{
    public class YeltProcessBenchmark : Benchmarks<YeltProcessTests>
    {
        public YeltProcessBenchmark() : base()
        {
        }

        public void RunBenchmarks()
        {
            Run_Benchmarks<YeltProcessTests>();
        }
    }

    [MemoryDiagnoser]
    public class YeltProcessTests
    {
        private const string _yeltRepoPath = @"C:\Data\Revo_Yelts";

        [Params(1, 2, 5, 10)]
        public int N;

        private (int, int) _portfolioRetroA = (1003, 274);
        private (int, int) _portfolioRetroB = (839, 247);
        private (int, int) _portfolioRetroC = (1003, 317);

        private Dictionary<(int, int), Arch.ILS.EconomicModel.YeltPartitionMapper> _mappers;
        private PortfolioRetroLayerYeltManager portfolioRetroLayerYeltManager;

        [GlobalSetup(Targets = new[] 
            { nameof(Get_YeltA_Benchmark)
            , nameof(Get_YeltB_Benchmark)
            , nameof(Get_YeltC_Benchmark)})]
        public void Setup()
        {
            IRevoRepository revoRepository = new RevoSnowflakeRepository(new SnowflakeConnectionStrings().RevoBermudaConnectionString);
            IRevoLayerLossRepository revoLayerLossRepository = new RevoLayerLossSnowflakeRepository(new SnowflakeConnectionStrings().RevoLayerLossBermudaConnectionString);
            portfolioRetroLayerYeltManager = new PortfolioRetroLayerYeltManager(ViewType.Projected, _yeltRepoPath, revoRepository, revoLayerLossRepository, [_portfolioRetroA, _portfolioRetroB, _portfolioRetroC], new HashSet<SegmentType> { SegmentType.PC });
            portfolioRetroLayerYeltManager.Initialise(true).Wait();

            _mappers = new Dictionary<(int, int), EconomicModel.YeltPartitionMapper>();
            foreach (var portfolioRetro in new[] { _portfolioRetroA, _portfolioRetroB, _portfolioRetroC })
            {
                if (!portfolioRetroLayerYeltManager.TryGetPortfolioRetroLayers(portfolioRetro, out var portfolioRetroLayers))
                    throw new Exception();

                YeltStorage yeltStorage = portfolioRetroLayerYeltManager.YeltStorage;
                Dictionary<int, Arch.ILS.EconomicModel.IYelt> layerYelt = new Dictionary<int, Arch.ILS.EconomicModel.IYelt>();
                foreach (var portfolioRetroLayer in portfolioRetroLayers)
                {
                    if (!portfolioRetroLayerYeltManager.TryGetLatestLayerLossAnalysis(portfolioRetroLayer.LayerId, RevoLossViewType.StressedView, out LayerLossAnalysis layerLossAnalysis))
                        continue;
                    if (yeltStorage.TryGetValue(layerLossAnalysis.LossAnalysisId, portfolioRetroLayer.LayerId, layerLossAnalysis.RowVersion, out Arch.ILS.EconomicModel.IYelt yelt))
                        layerYelt[portfolioRetroLayer.LayerId] = yelt;
                }

                /*Partition Layer Yelts*/
                Dictionary<Arch.ILS.EconomicModel.IYelt, Arch.ILS.EconomicModel.YeltPartitionReader> yeltReaders = new Dictionary<Arch.ILS.EconomicModel.IYelt, Arch.ILS.EconomicModel.YeltPartitionReader>();
                foreach (var yelt in layerYelt.Values)
                {
                    if (yelt.TotalEntryCount == 0)
                        continue;
                    Arch.ILS.EconomicModel.YeltPartitioner yeltPartitioner = new Arch.ILS.EconomicModel.YeltPartitioner(new Range[] { new Range(1, 366) }, yelt);
                    Arch.ILS.EconomicModel.YeltPartitionReader yeltPartitionLinkedListReader = Arch.ILS.EconomicModel.YeltPartitionReader.Initialise(yeltPartitioner);
                    int totalLength = yeltPartitionLinkedListReader.TotalLength;
                    if (totalLength != yelt.TotalEntryCount)
                        throw new Exception();
                    yeltReaders.Add(yelt, yeltPartitionLinkedListReader);
                }

                long[] mergedSortedKeys = Arch.ILS.EconomicModel.YeltPartitionMerge.Merge_Native(yeltReaders.Values);
                _mappers[portfolioRetro] = new Arch.ILS.EconomicModel.YeltPartitionMapper(yeltReaders.Values, mergedSortedKeys);
            }
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
        }

        [Benchmark(Description = "Get_Yelt_Portfolio_1003_Retro_274")]
        public void Get_YeltA_Benchmark()
        {
            double[] eventLosses = _mappers[_portfolioRetroA].ProcessPartitionsNative(1.0);
        }

        [Benchmark(Description = "Get_Yelt_Portfolio_839_Retro_247")]
        public void Get_YeltB_Benchmark()
        {
            double[] eventLosses = _mappers[_portfolioRetroA].ProcessPartitionsNative(1.0);
        }

        [Benchmark(Description = "Get_Yelt_Portfolio_1003_Retro_317")]
        public void Get_YeltC_Benchmark()
        {
            double[] eventLosses = _mappers[_portfolioRetroA].ProcessPartitionsNative(1.0);
        }

    }
}