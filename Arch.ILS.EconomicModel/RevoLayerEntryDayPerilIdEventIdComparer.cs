
namespace Arch.ILS.EconomicModel
{
    public class RevoLayerEntryDayPerilIdEventIdComparer : IComparer<RevoLayerYeltEntry>
    {
        public int Compare(RevoLayerYeltEntry x, RevoLayerYeltEntry y)
        {
            return GetDayPerilIdEventIdKey(x.Day, x.EventId, x.PerilId).CompareTo(GetDayPerilIdEventIdKey(y.Day, y.EventId, y.PerilId));//not expecting ties on year, day, eventId and peril from Revo.
        }

        public static ulong GetDayPerilIdEventIdKey(in short day, in long eventId, in byte perilId)
        {
            return (((ulong)day) << 39) | (((ulong)perilId) << 33) | (ulong)eventId;
        }

        public static short GetDay(in ulong dayPerilIdEventIdKey)
        {
            return (short)(dayPerilIdEventIdKey >> 39);
        }

        public static byte GetPerilId(in ulong dayPerilIdEventIdKey)
        {
            return (byte)((dayPerilIdEventIdKey >> 33) & 0x3F);
        }

        public static long GetEventId(in ulong dayPerilIdEventIdKey)
        {
            return (long)(dayPerilIdEventIdKey & 0x1FFFFFFFF);
        }
    }
}
