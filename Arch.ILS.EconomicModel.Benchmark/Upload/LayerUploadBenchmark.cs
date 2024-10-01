
using BenchmarkDotNet.Attributes;

namespace Arch.ILS.EconomicModel.Benchmark
{
    public class LayerUploadBenchmark : Benchmarks<LayerUploadTests>
    {
        public LayerUploadBenchmark() : base()
        {
        }

        public void RunBenchmarks()
        {
            Run_Benchmarks<LayerUploadTests>();
        }
    }

    [MemoryDiagnoser]
    public class LayerUploadTests
    {
        [Params(1_000, 10_000, 100_000, 1_000_000/*, 10_000_000, 100_000_000, 100_000_000*/)]
        public int N;

        private MockRevoLayerYeltRepository _revoLayerLossRepository;

        [GlobalSetup(Targets = new[] 
            { nameof(GetLayerYeltsFixedDictionary_Benchmark)
            , nameof(GetLayerYeltYearArray_Benchmark)
            , nameof(GetLayerYeltStandard_Benchmark)
            , nameof(GetLayerYeltStandardUnmanaged_Benchmark)
            , nameof(GetLayerYeltStandardUnsafe_Benchmark)
            , nameof(GetLayerYeltUnmanaged_Benchmark) })]
        public void Setup()
        {
            _revoLayerLossRepository = new MockRevoLayerYeltRepository(N);
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _revoLayerLossRepository = null;
        }

        [Benchmark]
        public void GetLayerYeltsFixedDictionary_Benchmark()
        {
            _revoLayerLossRepository.GetLayerYeltFixedDictionary();
        }

        [Benchmark]
        public void GetLayerYeltYearArray_Benchmark()
        {
            using var yelt = _revoLayerLossRepository.GetLayerYeltYearArray().Result;
        }

        [Benchmark]
        public void GetLayerYeltStandard_Benchmark()
        {
            using var yelt = _revoLayerLossRepository.GetLayerYeltStandard().Result;
        }

        [Benchmark]
        public void GetLayerYeltStandardUnmanaged_Benchmark()
        {
            using var yelt = _revoLayerLossRepository.GetLayerYeltStandardUnmanaged().Result;
        }

        [Benchmark]
        public void GetLayerYeltStandardUnsafe_Benchmark()
        {
            using var yelt = _revoLayerLossRepository.GetLayerYeltStandardUnsafe().Result;
        }

        [Benchmark]
        public void GetLayerYeltUnmanaged_Benchmark()
        {
            using var yelt = _revoLayerLossRepository.GetLayerYeltUnmanaged().Result;
        }
    }
}