
namespace Arch.ILS.EconomicModel
{
    public class RevoLayerEntryYearPerilIdEventIdComparer : IComparer<RevoLayerYeltEntry>
    {
        public int Compare(RevoLayerYeltEntry x, RevoLayerYeltEntry y)
        {
            return GetYearPerilIdEventIdKey(x.Year, x.EventId, x.PerilId).CompareTo(GetYearPerilIdEventIdKey(y.Year, y.EventId, y.PerilId));//not expecting ties on year, day, eventId, Peril from Revo.
        }

        public static ulong GetYearPerilIdEventIdKey(in short year, in int eventId, in byte perilId)
        {
            return (((ulong)year) << 33) |  (((ulong)perilId) << 32) | (uint)eventId;
        }

        public static short GetYear(in ulong yearPerilIdEventIdKey)
        {
            return (short)(yearPerilIdEventIdKey >> 33);
        }

        public static int GetEventId(in ulong yearPerilIdEventIdKey)
        {
            return (int)yearPerilIdEventIdKey;
        }

        public static byte GetPerilId(in ulong yearPerilIdEventIdKey)
        {
            return (byte)(yearPerilIdEventIdKey >> 32);
        }
    }
}
