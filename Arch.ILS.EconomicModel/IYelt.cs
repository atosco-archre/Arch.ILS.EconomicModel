
using System.Runtime.CompilerServices;

namespace Arch.EconomicModel
{
    public interface IYelt
    {
        int LossAnalysisId { get; }
        int LayerId { get; }
        int BufferCount { get; }
        int TotalEntryCount { get; }

        ReadOnlySpan<long> YearDayEventIdKeys(in uint i);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ReadOnlySpan<short> Days(in uint i);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ReadOnlySpan<double> LossPcts(in uint i);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ReadOnlySpan<double> RPs(in uint i);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ReadOnlySpan<double> RBs(in uint i);
    }
}
