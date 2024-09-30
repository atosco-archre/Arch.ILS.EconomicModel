
namespace Arch.ILS.EconomicModel.Benchmark
{
    public class RevoLayerEntryYearDayEventIdComparer : IComparer<RevoLayerYeltEntry>
    {
        public int Compare(RevoLayerYeltEntry x, RevoLayerYeltEntry y)
        {
            return GetYearDayEventIdKey(x.Year, x.Day, x.EventId).CompareTo(GetYearDayEventIdKey(y.Year, y.Day, y.EventId));//not expecting ties on year, day, eventId from Revo.
        }

        public static ulong GetYearDayEventIdKey(in int year, in short day, in int eventId)
        {
            return (((ulong)year) << 48) | (((ulong)day) << 32) | (uint)eventId;
        }

        public static int GetYear(in ulong yearDayEventIdKey)
        {
            return (int)(yearDayEventIdKey >> 48);
        }

        public static short GetDay(in ulong yearDayEventIdKey)
        {
            return (short)(yearDayEventIdKey >> 32);
        }

        public static int GetEventId(in ulong yearDayEventIdKey)
        {
            return (int)yearDayEventIdKey;
        }
    }
}
