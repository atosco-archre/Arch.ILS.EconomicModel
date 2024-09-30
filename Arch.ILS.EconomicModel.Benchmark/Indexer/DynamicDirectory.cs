
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

using Arch.ILS.Core;

namespace Arch.ILS.EconomicModel.Benchmark
{
    [DebuggerDisplay("Count = {" + nameof(Count) + "}")]
    public unsafe struct DynamicDirectory<TKey, TValue>
        where TKey : struct, IEquatable<TKey>
    {
        private const int MinSize = 8419;
        private static readonly int[] Primes =
         {
             3, 7, 11, 17, 23, 29, 37, 47, 59, 71, 89, 107, 131, 163, 197, 239, 293, 353, 431, 521, 631, 761, 919,
             1103, 1327, 1597, 1931, 2333, 2801, 3371, 4049, 4861, 5839, 7013, 8419, 10103, 12143, 14591,
             17519, 21023, 25229, 30293, 36353, 43627, 52361, 62851, 75431, 90523, 108631, 130363, 156437,
             187751, 225307, 270371, 324449, 389357, 467237, 560689, 672827, 807403, 968897, 1162687, 1395263,
             1674319, 2009191, 2411033, 2893249, 3471899, 4166287, 4999559, 5999471, 7199369
         };
        private static Entry _default;

        // 1-based index into _entries; 0 means empty
        private readonly int[] _buckets;
        private readonly Entry[] _entries;
        private readonly uint _size;
        private int _count;

        public DynamicDirectory(int fixedCapacity)
        {
            if(fixedCapacity < (MinSize >> 1))
                _size = MinSize;
            else
            {
                int sizeIndex = Array.BinarySearch(Primes, fixedCapacity);
                _size = (uint)((sizeIndex < 0) ? 
                    Primes[(~sizeIndex) == Primes.Length - 1 ? (~sizeIndex) : (~sizeIndex) + 1] : 
                    Primes[sizeIndex == Primes.Length - 1 ? sizeIndex : sizeIndex + 1]);
            }

            _buckets = new int[_size];
            _entries = new Entry[fixedCapacity];
        }

        [StructLayout(LayoutKind.Auto)]
        public record struct Entry
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
            uint bucketIndex = hashCode % _size;
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
        public ref Entry GetNextRef(ref TKey key, ref readonly Entry entry)
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
            uint bucketIndex = hashCode % _size;
            ref Entry entry = ref _entries.GetAtUnsafe((uint)index);

            entry.hashCode = hashCode;
            entry.next = (_buckets.GetAtUnsafe(bucketIndex) - 1); // Value in _buckets is 1-based
            entry.key = key;
            entry.value = value;
            _buckets.GetAtUnsafe(bucketIndex) = index + 1; // Value in _buckets is 1-based
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public bool Move(TKey oldKey, TKey newKey, ref Entry entry)
        {
            Entry[] entries = _entries;
            uint oldHashCode = (uint)oldKey.GetHashCode(); // Constrained call
            uint oldBucketIndex = oldHashCode % _size;
            int bucket = _buckets.GetAtUnsafe(oldBucketIndex);
            nuint i = (uint)bucket - 1; // Value in _buckets is 1-based
            ref Entry previousEntry = ref _default;

            while (true)
            {
                if (i >= (uint)entries.Length) // Eliminates bound checks
                    return false;

                if (entries.GetAtUnsafe(i).hashCode == oldHashCode && entries.GetAtUnsafe(i).Equals(entry))
                { 
                    if(previousEntry.Equals(_default))
                        _buckets.GetAtUnsafe(oldBucketIndex) = entries.GetAtUnsafe(i).next + 1;
                    else
                        previousEntry.next = entries.GetAtUnsafe(i).next;

                    break;
                }
                previousEntry = ref entries.GetAtUnsafe(i);
                i = (uint)previousEntry.next;
            }

            uint newHashCode = (uint)newKey.GetHashCode(); // Constrained call
            uint bucketIndex = newHashCode % _size;
            entry.hashCode = newHashCode;
            entry.next = (_buckets.GetAtUnsafe(bucketIndex) - 1); // Value in _buckets is 1-based
            entry.key = newKey;
            _buckets.GetAtUnsafe(bucketIndex) = (int)i + 1; // Value in _buckets is 1-based

            return true;
        }
    }
}