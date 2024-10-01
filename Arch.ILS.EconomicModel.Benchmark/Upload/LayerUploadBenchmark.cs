
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
        public async void GetLayerYeltsFixedDictionary_Benchmark()
        {
            await _revoLayerLossRepository.GetLayerYeltFixedDictionary();
        }

        [Benchmark]
        public async void GetLayerYeltYearArray_Benchmark()
        {
            using var yelt = await _revoLayerLossRepository.GetLayerYeltYearArray();
        }

        [Benchmark]
        public async void GetLayerYeltStandard_Benchmark()
        {
            using var yelt = await _revoLayerLossRepository.GetLayerYeltStandard();
        }

        [Benchmark]
        public async void GetLayerYeltStandardUnmanaged_Benchmark()
        {
            using var yelt = await _revoLayerLossRepository.GetLayerYeltStandardUnmanaged();
        }

        [Benchmark]
        public async void GetLayerYeltStandardUnsafe_Benchmark()
        {
            using var yelt = await _revoLayerLossRepository.GetLayerYeltStandardUnsafe();
        }

        [Benchmark]
        public async void GetLayerYeltUnmanaged_Benchmark()
        {
            using var yelt = await _revoLayerLossRepository.GetLayerYeltUnmanaged();
        }
    }
}