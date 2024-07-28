
namespace Arch.ILS.EconomicModel.Benchmark
{
    public interface IBenchmarkYelt
    {
        int LossAnalysisId { get; }
        int LayerId { get; }

        int YearBufferCount { get; }
        int BufferCount { get; }

        ReadOnlySpan<short> DistinctYears(in uint i);

        ReadOnlySpan<int> YearRepeatCount(in uint i);

        ReadOnlySpan<short> Days(in uint i);

        ReadOnlySpan<int> EventIds(in uint i);

        ReadOnlySpan<double> LossPcts(in uint i);

        ReadOnlySpan<double> RPs(in uint i);

        ReadOnlySpan<double> RBs(in uint i);
    }
}
