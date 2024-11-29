
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Arch.ILS.EconomicModel
{
    [StructLayout(LayoutKind.Auto)]
    public unsafe struct RevoLayerYeltEntry
    {
        public RevoLayerYeltEntry()
        {
        }

        public short Year;

        public long EventId;

        public byte PerilId;

        public short Day;

        public double LossPct;

        public double RP;

        public double RB;


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe short* GetYear()
        {
            return (short*)Unsafe.AsPointer(ref Year);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe long* GetEventId()
        {
            /*TODO:temporary fix to too long eventids*/
            if ((((ulong)EventId) & 0xFFFFFFFE00000000L) > 0)
                EventId = (EventId & 0x1FFFFFFFFL);
            return (long*)Unsafe.AsPointer(ref EventId);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe byte* GetPerilId()
        {
            return (byte*)Unsafe.AsPointer(ref PerilId);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe short* GetDay()
        {
            return (short*)Unsafe.AsPointer(ref Day);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe double* GetLossPct()
        {
            return (double*)Unsafe.AsPointer(ref LossPct);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe double* GetRP()
        {
            return (double*)Unsafe.AsPointer(ref RP);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe double* GetRB()
        {
            return (double*)Unsafe.AsPointer(ref RB);
        }
    }
}
