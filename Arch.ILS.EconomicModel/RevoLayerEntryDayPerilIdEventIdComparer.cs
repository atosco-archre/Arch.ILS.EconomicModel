
namespace Arch.ILS.EconomicModel
{
    public class RevoLayerEntryDayPerilIdEventIdComparer : IComparer<RevoLayerYeltEntry>
    {
        public int Compare(RevoLayerYeltEntry x, RevoLayerYeltEntry y)
        {
            return GetDayPerilIdEventIdKey(x.Day, x.EventId, x.PerilId).CompareTo(GetDayPerilIdEventIdKey(y.Day, y.EventId, y.PerilId));//not expecting ties on year, day, eventId and peril from Revo.
        }

        public static ulong GetDayPerilIdEventIdKey(in short day, in int eventId, in byte perilId)
        {
            return (((ulong)day) << 33) | (((ulong)perilId) << 32) | (uint)eventId;
        }

        public static short GetDay(in ulong dayPerilIdEventIdKey)
        {
            return (short)(dayPerilIdEventIdKey >> 33);
        }

        public static int GetEventId(in ulong dayPerilIdEventIdKey)
        {
            return (int)dayPerilIdEventIdKey;
        }

        public static byte GetPerilId(in ulong dayPerilIdEventIdKey)
        {
            return (byte)(dayPerilIdEventIdKey >> 32);
        }
    }
}
