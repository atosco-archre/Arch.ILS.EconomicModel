
using System.Runtime.CompilerServices;

namespace Arch.ILS.EconomicModel
{
    public class RevoLayerEntryYearPerilIdEventIdComparer : IComparer<RevoLayerYeltEntry>
    {
        public int Compare(RevoLayerYeltEntry x, RevoLayerYeltEntry y)
        {
            return GetYearPerilIdEventIdKey(x.Year, x.EventId, x.PerilId).CompareTo(GetYearPerilIdEventIdKey(y.Year, y.EventId, y.PerilId));//not expecting ties on year, day, eventId, Peril from Revo.
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong GetYearPerilIdEventIdKey(in short year, in long eventId, in byte perilId)
        {
            return (((ulong)year) << 39) |  (((ulong)perilId) << 33) | (ulong)eventId;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static short GetYear(in ulong yearPerilIdEventIdKey)
        {
            return (short)(yearPerilIdEventIdKey >> 39);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte GetPerilId(in ulong yearPerilIdEventIdKey)
        {
            return (byte)((yearPerilIdEventIdKey >> 33) & 0x3F);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long GetEventId(in ulong yearPerilIdEventIdKey)
        {
            return (long)(yearPerilIdEventIdKey & 0x1FFFFFFFF);
        }
    }
}
