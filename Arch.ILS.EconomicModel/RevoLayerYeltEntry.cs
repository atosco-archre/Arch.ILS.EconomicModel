
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Arch.EconomicModel
{
    [StructLayout(LayoutKind.Auto)]
    public unsafe struct RevoLayerYeltEntry
    {
        public RevoLayerYeltEntry()
        {
        }

        public short Year;

        public int EventId;

        public string Peril = string.Empty;

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
        public unsafe int* GetEventId()
        {
            return (int*)Unsafe.AsPointer(ref EventId);
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
