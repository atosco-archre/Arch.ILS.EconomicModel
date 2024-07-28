
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

        private MockRevoLayerLossRepository _revoLayerLossRepository;

        [GlobalSetup(Targets = new[] 
            { nameof(GetLayerYeltsFixedDictionary_Benchmark)
            , nameof(GetLayerYeltYearArray_Benchmark)
            , nameof(GetLayerYeltStandard_Benchmark)
            , nameof(GetLayerYeltStandardUnmanaged_Benchmark)
            , nameof(GetLayerYeltStandardUnsafe_Benchmark)
            , nameof(GetLayerYeltUnmanaged_Benchmark) })]
        public void Setup()
        {
            _revoLayerLossRepository = new MockRevoLayerLossRepository(N);
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
            _revoLayerLossRepository.GetLayerYeltYearArray();
        }

        [Benchmark]
        public void GetLayerYeltStandard_Benchmark()
        {
            _revoLayerLossRepository.GetLayerYeltStandard();
        }

        [Benchmark]
        public void GetLayerYeltStandardUnmanaged_Benchmark()
        {
            _revoLayerLossRepository.GetLayerYeltStandardUnmanaged();
        }

        [Benchmark]
        public void GetLayerYeltStandardUnsafe_Benchmark()
        {
            _revoLayerLossRepository.GetLayerYeltStandardUnsafe();
        }

        [Benchmark]
        public void GetLayerYeltUnmanaged_Benchmark()
        {
            _revoLayerLossRepository.GetLayerYeltUnmanaged();
        }
    }
}