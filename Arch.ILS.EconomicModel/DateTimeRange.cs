
using System.Runtime.CompilerServices;

namespace Arch.ILS.EconomicModel
{
    public ref struct DateTimeRange
    {
        public DateTimeRange(DateTime start, DateTime end)
        {
            StartInclusive = start; 
            EndInclusive = end;
        }

        public DateTime StartInclusive { get; init; }
        public DateTime EndInclusive { get; init; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryGetLeftNonOverlap(ref readonly DateTimeRange partitioner, out DateTimeRange leftNonOverlap)
        {
            leftNonOverlap = new DateTimeRange(StartInclusive, EndInclusive < partitioner.StartInclusive ? EndInclusive : partitioner.StartInclusive.AddDays(-1));
            return IsValid(ref leftNonOverlap);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetRightNonOverlap(ref readonly DateTimeRange partitioner, out DateTimeRange rightNonOverlap)
        {
            rightNonOverlap = new DateTimeRange(partitioner.EndInclusive < StartInclusive ? StartInclusive : partitioner.EndInclusive.AddDays(1), EndInclusive);
            return IsValid(ref rightNonOverlap);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetOverlap(ref readonly DateTimeRange partitioner, out DateTimeRange overlap)
        {
            overlap = StartInclusive < partitioner.StartInclusive
                ? (EndInclusive < partitioner.EndInclusive
                        ? new DateTimeRange(partitioner.StartInclusive, EndInclusive)
                        : new DateTimeRange(partitioner.StartInclusive, partitioner.EndInclusive))
                : (EndInclusive < partitioner.EndInclusive
                        ? new DateTimeRange(StartInclusive, EndInclusive)
                        : new DateTimeRange(StartInclusive, partitioner.EndInclusive));
            return IsValid(ref overlap);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsValid(ref readonly DateTimeRange range)
        {
            return range.StartInclusive <= range.EndInclusive;
        }
    }
}
