
using Arch.ILS.Core;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Arch.ILS.EconomicModel.Benchmark
{
    [DebuggerDisplay("Count = {" + nameof(Count) + "}")]
    public unsafe struct FixedDirectory<TKey, TValue>
        where TKey : struct, IEquatable<TKey>
    {
        // Pick a Prime for Size
        // private static readonly int[] Primes =
        // {
        //     3, 7, 11, 17, 23, 29, 37, 47, 59, 71, 89, 107, 131, 163, 197, 239, 293, 353, 431, 521, 631, 761, 919,
        //     1103, 1327, 1597, 1931, 2333, 2801, 3371, 4049, 4861, 5839, 7013, 8419, 10103, 12143, 14591,
        //     17519, 21023, 25229, 30293, 36353, 43627, 52361, 62851, 75431, 90523, 108631, 130363, 156437,
        //     187751, 225307, 270371, 324449, 389357, 467237, 560689, 672827, 807403, 968897, 1162687, 1395263,
        //     1674319, 2009191, 2411033, 2893249, 3471899, 4166287, 4999559, 5999471, 7199369
        // };

        private const int Size = 8419;
        public const int MaxCount = 1 << 13;
        private static Entry _default;

        // 1-based index into _entries; 0 means empty
        private readonly int[] _buckets = new int[Size];
        private readonly Entry[] _entries = new Entry[Size];
        private int _count;

        public FixedDirectory()
        {
        }

        [StructLayout(LayoutKind.Auto)]
        public struct Entry
        {
            public uint hashCode;
            public int next;
            public TKey key;
            public TValue value;
        }

        public static Entry Default => _default;

        /// <summary>
        /// Count of entries in the dictionary.
        /// </summary>
        public int Count => _count;

        /// <summary>
        /// Clears the dictionary. Note that this invalidates any active enumerators.
        /// </summary>
        public void Clear()
        {
            int count = _count;
            if (count > 0)
            {
                Debug.Assert(_buckets != null, "_buckets should be non-null");
                Debug.Assert(_entries != null, "_entries should be non-null");

                Array.Clear(_buckets);
                Array.Clear(_entries, 0, count);
                _count = 0;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public ref Entry GetFirstRef(ref TKey key)
        {
            Entry[] entries = _entries;
            uint hashCode = (uint)key.GetHashCode(); // Constrained call
            uint bucketIndex = hashCode % Size;
            int bucket = _buckets.GetAtUnsafe(bucketIndex);
            nuint i = (uint)bucket - 1; // Value in _buckets is 1-based

            while (true)
            {
                if (i >= (uint)entries.Length) // Eliminates bound checks
                    break;

                if (entries.GetAtUnsafe(i).hashCode == hashCode && entries.GetAtUnsafe(i).key.Equals(key))
                    return ref entries.GetAtUnsafe(i);

                i = (uint)entries.GetAtUnsafe(i).next;
            }

            return ref _default;
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public ref Entry GetNextRef(ref TKey key, ref Entry entry)
        {
            Entry[] entries = _entries;
            uint hashCode = (uint)key.GetHashCode(); // Constrained call
            nuint i = (uint)entry.next; // Value in _buckets is 1-based

            while (true)
            {
                if (i >= (uint)entries.Length) // Eliminates bound checks
                    break;

                if (entries.GetAtUnsafe(i).hashCode == hashCode && entries.GetAtUnsafe(i).key.Equals(key))
                    return ref entries.GetAtUnsafe(i);

                i = (uint)entries.GetAtUnsafe(i).next;
            }

            return ref _default;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void Add(TKey key, ref TValue value)
        {
            int index = _count++;
            uint hashCode = (uint)key.GetHashCode(); // Constrained call
            uint bucketIndex = hashCode % Size;
            ref Entry entry = ref _entries.GetAtUnsafe((uint)index);

            entry.hashCode = hashCode;
            entry.next = (_buckets.GetAtUnsafe(bucketIndex) - 1); // Value in _buckets is 1-based
            entry.key = key;
            entry.value = value;

            _buckets.GetAtUnsafe(bucketIndex) = index + 1; // Value in _buckets is 1-based
        }
    }
}