using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;

namespace Arch.ILS.EconomicModel.Benchmark
{
    public readonly unsafe struct Int32Span : IEquatable<Int32Span>
    {
        internal readonly int* Pointer;

        public Int32Span(int* pointer)
        {
            Pointer = pointer;
        }

        /// <summary>
        /// Ref MUST be to a pinned or native memory
        /// </summary>
        public Int32Span(ref int pointer)
        {
            Pointer = (int*)Unsafe.AsPointer(ref pointer);
        }

        public ReadOnlySpan<int> Span
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new(Pointer, 4);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(Int32Span other)
        {
            byte* ptr = (byte*)Pointer;
            byte* otherPtr = (byte*)other.Pointer;
            if(ptr == otherPtr) return true;
            if (*ptr++ != *otherPtr++) return false;
            if (*ptr++ != *otherPtr++) return false;
            if (*ptr++ != *otherPtr++) return false;
            if (*ptr != *otherPtr) return false;
            return true;
        }

        public override bool Equals(object? obj) => obj is Int32Span other && Equals(other);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode()
        {
            return *Pointer;
        }

        public override string ToString() => new((sbyte*)Pointer, 0, 4, Encoding.UTF8);
    }
}