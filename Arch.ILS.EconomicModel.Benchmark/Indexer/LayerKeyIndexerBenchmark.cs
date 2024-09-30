
using BenchmarkDotNet.Attributes;
using Microsoft.Diagnostics.Tracing.StackSources;

namespace Arch.ILS.EconomicModel.Benchmark
{
    public class LayerKeyIndexerBenchmark : Benchmarks<LayerKeyIndexerTests>
    {
        public LayerKeyIndexerBenchmark() : base()
        {
        }

        public void RunBenchmarks()
        {
            Run_Benchmarks<LayerKeyIndexerTests>();
        }
    }

    [MemoryDiagnoser]
    public class LayerKeyIndexerTests
    {
        [Params(1_000, 10_000, 100_000, 1_000_000, 10_000_000/*, 100_000_000, 100_000_000*/)]
        public int N;

        private MockRevoLayerDayYeltRepository _revoLayerDayYeltRepository;
        private RevoLayerDayYeltVectorised _dayYelt0;
        private RevoLayerDayYeltVectorised _dayYelt1;
        private YeltPartitionMapper _mapper;

        [GlobalSetup(Targets = new[] 
            { nameof(GetIndexFromDictionary_Benchmark)
            , nameof(GetIndexFromFixedDirectory_Benchmark)
            , nameof(GetIndexFromDynamicDirectory_Benchmark)
            , nameof(GetIndexFromDynamicDirectory2_Benchmark)
            , nameof(GetIndexFromDynamicDirectory3_Benchmark)
            , nameof(GetIndexFromDynamicDirectory5_Benchmark) })]
        public unsafe void Setup()
        {
            _revoLayerDayYeltRepository = new MockRevoLayerDayYeltRepository(N);
            _dayYelt0 = _revoLayerDayYeltRepository.GetLayerYelt0().Result;
            _dayYelt1 = _revoLayerDayYeltRepository.GetLayerYelt1().Result;
            Range[] ranges = [new Range(2, 50)];
            YeltPartitioner yeltPartitioner0 = new YeltPartitioner(ranges, _dayYelt0);
            YeltPartitionReader yeltPartitionLinkedListReader0 = YeltPartitionReader.Initialise(yeltPartitioner0);
            YeltPartitioner yeltPartitioner1 = new YeltPartitioner(ranges, _dayYelt1);
            YeltPartitionReader yeltPartitionLinkedListReader1 = YeltPartitionReader.Initialise(yeltPartitioner1);
            long[] sortedKeys = new long[yeltPartitionLinkedListReader0.TotalLength + yeltPartitionLinkedListReader1.TotalLength];
            fixed (long* keysPtr = sortedKeys)
            {
                long* ptr = keysPtr;
                int keyCount = YeltPartitionMerge.Merge_ScalarOptimised_2(yeltPartitionLinkedListReader0.Head, yeltPartitionLinkedListReader1.Head, ptr);
                Array.Resize(ref sortedKeys, keyCount);
            }
            _mapper = new YeltPartitionMapper( [ yeltPartitionLinkedListReader0, yeltPartitionLinkedListReader1 ], sortedKeys);
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _revoLayerDayYeltRepository = null;
        }

        [Benchmark]
        public void GetIndexFromDictionary_Benchmark()
        {
            _mapper.Reset();
            _mapper.MapKeysFromDictionary();
        }

        [Benchmark]
        public void GetIndexFromFixedDirectory_Benchmark()
        {
            _mapper.Reset();
            _mapper.MapKeysFromFixedDirectory();
        }

        [Benchmark]
        public void GetIndexFromDynamicDirectory_Benchmark()
        {
            _mapper.Reset();
            _mapper.MapKeysFromDynamicDirectory();
        }

        [Benchmark]
        public void GetIndexFromDynamicDirectory2_Benchmark()
        {
            _mapper.Reset();
            _mapper.MapKeysFromDynamicDirectory2();
        }

        [Benchmark]
        public void GetIndexFromDynamicDirectory3_Benchmark()
        {
            _mapper.Reset();
            _mapper.MapKeysFromDynamicDirectory3();
        }

        [Benchmark]
        public void GetIndexFromDynamicDirectory5_Benchmark()
        {
            _mapper.Reset();
            _mapper.MapKeysFromDynamicDirectory5();
        }
    }
}