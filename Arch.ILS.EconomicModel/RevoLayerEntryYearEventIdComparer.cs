
namespace Arch.ILS.EconomicModel
{
    public class RevoLayerEntryYearEventIdComparer : IComparer<RevoLayerYeltEntry>
    {
        public int Compare(RevoLayerYeltEntry x, RevoLayerYeltEntry y)
        {
            return GetYearEventIdKey(x.Year, x.EventId).CompareTo(GetYearEventIdKey(y.Year, y.EventId));//not expecting ties on year, day, eventId from Revo.
        }

        public static ulong GetYearEventIdKey(in short year, in int eventId)
        {
            return (((ulong)year) << 32) | (uint)eventId;
        }

        public static short GetYear(in ulong yearEventIdKey)
        {
            return (short)(yearEventIdKey >> 32);
        }

        public static int GetEventId(in ulong yearEventIdKey)
        {
            return (int)yearEventIdKey;
        }
    }
}
