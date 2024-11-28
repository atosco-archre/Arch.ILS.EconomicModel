
using System.Runtime.InteropServices;

namespace Arch.ILS.EconomicModel
{
    [StructLayout(LayoutKind.Explicit, Size = 13)]
    public struct YeltKey
    {
        [FieldOffset(0)] long eventId;
        [FieldOffset(8)] byte perilId;
        [FieldOffset(9)] short year;
        [FieldOffset(11)] short day;

        public long EventId 
        {
            get { return eventId;} 
            set{ eventId = value; } 
        }

        public byte PerilId
        {
            get { return perilId; }
            set { perilId = value; }
        }

        public short Year
        {
            get { return year; }
            set { year = value; }
        }

        public short Day
        {
            get { return day; }
            set { day = value; }
        }
    }
}
