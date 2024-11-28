
using System.Runtime.CompilerServices;

namespace Arch.ILS.EconomicModel
{
    public interface IYelt
    {
        int LossAnalysisId { get; }
        int LayerId { get; }
        long RowVersion { get; set; }
        int BufferCount { get; }
        int TotalEntryCount { get; }
        bool HasRP { get; }
        bool HasRB { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ReadOnlySpan<long> YearDayEventIdPerilIdKeys(in uint i);

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
