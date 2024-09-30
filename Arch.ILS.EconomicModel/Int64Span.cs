
using System.Runtime.CompilerServices;
using System.Text;

namespace Arch.ILS.EconomicModel
{
    public readonly unsafe struct Int64Span : IEquatable<Int64Span>
    {
        internal readonly long* Pointer;

        public Int64Span(long* pointer)
        {
            Pointer = pointer;
        }

        /// <summary>
        /// Ref MUST be to a pinned or native memory
        /// </summary>
        public Int64Span(ref long pointer)
        {
            Pointer = (long*)Unsafe.AsPointer(ref pointer);
        }

        public ReadOnlySpan<long> Span
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new(Pointer, 8);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(Int64Span other)
        {
            byte* ptr = (byte*)Pointer;
            byte* otherPtr = (byte*)other.Pointer;

            return ptr == otherPtr
                || ((*ptr++ == *otherPtr++)
                    && (*ptr++ == *otherPtr++)
                    && (*ptr++ == *otherPtr++)
                    && (*ptr++ == *otherPtr++)
                    && (*ptr++ == *otherPtr++)
                    && (*ptr++ == *otherPtr++)
                    && (*ptr++ == *otherPtr++)
                    && (*ptr == *otherPtr));
        }

        public override bool Equals(object? obj) => obj is Int64Span other && Equals(other);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode()
        {
            return unchecked((int)*Pointer) ^ ((int)((*Pointer) >> 32));
        }

        public override string ToString() => new((sbyte*)Pointer, 0, 8, Encoding.UTF8);
    }
}