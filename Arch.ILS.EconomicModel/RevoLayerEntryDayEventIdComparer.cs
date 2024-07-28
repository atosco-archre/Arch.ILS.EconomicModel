
namespace Arch.ILS.EconomicModel
{
    public class RevoLayerEntryDayEventIdComparer : IComparer<RevoLayerYeltEntry>
    {
        public int Compare(RevoLayerYeltEntry x, RevoLayerYeltEntry y)
        {
            return GetDayEventIdKey(x.Day, x.EventId).CompareTo(GetDayEventIdKey(y.Day, y.EventId));//not expecting ties on year, day, eventId from Revo.
        }

        public static ulong GetDayEventIdKey(in short day, in int eventId)
        {
            return (((ulong)day) << 32) | (uint)eventId;
        }

        public static short GetDay(in ulong dayEventIdKey)
        {
            return (short)(dayEventIdKey >> 32);
        }

        public static int GetEventId(in ulong dayEventIdKey)
        {
            return (int)dayEventIdKey;
        }
    }
}
