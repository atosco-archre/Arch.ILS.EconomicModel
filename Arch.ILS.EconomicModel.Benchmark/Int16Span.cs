using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;

namespace Arch.ILS.EconomicModel.Benchmark
{
    public readonly unsafe struct Int16Span : IEquatable<Int16Span>
    {
        internal readonly short* Pointer;

        public Int16Span(short* pointer)
        {
            Pointer = pointer;
        }

        /// <summary>
        /// Ref MUST be to a pinned or native memory
        /// </summary>
        public Int16Span(ref short pointer)
        {
            Pointer = (short*)Unsafe.AsPointer(ref pointer);
        }

        public ReadOnlySpan<short> Span
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new(Pointer, 2);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(Int16Span other)
        {
            byte* ptr = (byte*)Pointer;
            byte* otherPtr = (byte*)other.Pointer;
            if(ptr == otherPtr) return true;
            if (*ptr++ != *otherPtr++) return false;
            if (*ptr != *otherPtr) return false;
            return true;
        }

        public override bool Equals(object? obj) => obj is Int16Span other && Equals(other);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode()
        {
            return *Pointer;
        }

        public override string ToString() => new((sbyte*)Pointer, 0, 2, Encoding.UTF8);
    }
}