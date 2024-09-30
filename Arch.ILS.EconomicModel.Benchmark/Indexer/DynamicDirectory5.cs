
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Arch.ILS.EconomicModel.Benchmark
{
    [DebuggerDisplay("Count = {" + nameof(Count) + "}")]
    public unsafe struct DynamicDirectory5<TKey, TValue> : IDisposable
        where TKey : struct, IEquatable<TKey>
    {
        public const int CacheLineSize = 64;
        private const int MinSize = 8419;
        private static readonly int[] Primes =
         {
             3, 7, 11, 17, 23, 29, 37, 47, 59, 71, 89, 107, 131, 163, 197, 239, 293, 353, 431, 521, 631, 761, 919,
             1103, 1327, 1597, 1931, 2333, 2801, 3371, 4049, 4861, 5839, 7013, 8419, 10007, 10103, 12143, 14591,
             17519, 21023, 25229, 30293, 36353, 43627, 52361, 62851, 75431, 90523, 100003, 108631, 130363, 156437,
             187751, 225307, 270371, 324449, 389357, 467237, 560689, 672827, 807403, 968897, 1162687, 1395263,
             1674319, 2009191, 2411033, 2893249, 3471899, 4166287, 4999559, 5999471, 7199369
         };

        // 1-based index into _entries; 0 means empty
        private Entry** _buckets;
        private Entry* _entries;
        private readonly nuint _bucketsByteCount;
        private readonly nuint _entriesByteCount;
        private static readonly uint _entryAlignment;
        private readonly uint _size;
        private int _count;
        private bool _disposed;

        static DynamicDirectory5()
        {
            _entryAlignment = BitOperations.RoundUpToPowerOf2((uint)Unsafe.SizeOf<Entry>());
        }

        public DynamicDirectory5(int fixedCapacity)
        {
            _disposed = false;
            if(fixedCapacity < (MinSize >> 1))
                _size = MinSize;
            else
            {
                int sizeIndex = Array.BinarySearch(Primes, fixedCapacity);
                _size = (uint)((sizeIndex < 0) ? 
                    Primes[(~sizeIndex) == Primes.Length - 1 ? (~sizeIndex) : (~sizeIndex) + 1] : 
                    Primes[sizeIndex == Primes.Length - 1 ? sizeIndex : sizeIndex + 1]);
            }

            _entriesByteCount = (nuint)(Unsafe.SizeOf<Entry>() * fixedCapacity);
            _entries = (Entry*)NativeMemory.AlignedAlloc(_entriesByteCount, _entryAlignment);
#if DEBUG
        NativeMemory.Clear(_entries, _entriesByteCount); // Not absolutely necessary so only DEBUG
#endif

            _bucketsByteCount = (nuint)(Unsafe.SizeOf<IntPtr>() * _size);
            _buckets = (Entry**)NativeMemory.AlignedAlloc(_bucketsByteCount, (nuint)Unsafe.SizeOf<IntPtr>());
            NativeMemory.Clear(_buckets, _bucketsByteCount);
        }

        [StructLayout(LayoutKind.Auto)]
        public struct Entry
        {
            public uint hashCode;
            public Entry* previous;
            public Entry* next;
            public TKey key;
            public TValue value;
        }

        /// <summary>
        /// Count of entries in the dictionary.
        /// </summary>
        public int Count => _count;

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public Entry* GetFirstRef(ref readonly TKey key)
        {
            uint hashCode = (uint)key.GetHashCode(); // Constrained call
            uint bucketIndex = hashCode % _size;
            Entry* bucketEntry = *(_buckets + bucketIndex);

            while (bucketEntry != null)
            {
                if (bucketEntry->hashCode == hashCode && bucketEntry->key.Equals(key))
                    return bucketEntry;

                bucketEntry = bucketEntry->next;
            }

            return null;
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public Entry* GetNextRef(ref readonly TKey key, Entry* entry)
        {
            uint hashCode = (uint)key.GetHashCode(); // Constrained call
            Entry* nextEntry = entry->next; 

            while (nextEntry != null)
            {
                if (nextEntry->hashCode == hashCode && nextEntry->key.Equals(key))
                    return nextEntry;

                nextEntry = nextEntry->next;
            }

            return null;
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public void Add(ref readonly TKey key, ref readonly TValue value)
        {
            int index = _count++;
            uint hashCode = (uint)key.GetHashCode(); // Constrained call
            uint bucketIndex = hashCode % _size;
            Entry* entry = _entries + index;
            *entry = new Entry();

            entry->hashCode = hashCode;
            entry->previous = null;
            entry->next = *(_buckets + bucketIndex);
            if(entry->next != null)
                entry->next->previous = entry;
            entry->key = key;
            entry->value = value;
            *(_buckets + bucketIndex) = entry;
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public bool Move(ref readonly TKey newKey, Entry* entry)
        {
            uint oldHashCode = entry->hashCode; // Constrained call
            uint oldBucketIndex = oldHashCode % _size;

            if (entry->previous == null)
                *(_buckets + oldBucketIndex) = entry->next;
            else
                entry->previous->next = entry->next;

            if (entry->next != null)
                entry->next->previous = entry->previous;

            uint newHashCode = (uint)newKey.GetHashCode(); // Constrained call
            uint bucketIndex = newHashCode % _size;
            entry->hashCode = newHashCode;
            entry->previous = null;
            entry->next = *(_buckets + bucketIndex);
            if (entry->next != null)
                entry->next->previous = entry;
            entry->key = newKey;
            *(_buckets + bucketIndex) = entry;

            return true;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        // Dispose(bool disposing) executes in two distinct scenarios.
        // If disposing equals true, the method has been called directly
        // or indirectly by a user's code. Managed and unmanaged resources
        // can be disposed.
        // If disposing equals false, the method has been called by the 
        // runtime from inside the finalizer and you should not reference 
        // other objects. Only unmanaged resources can be disposed.
        private void Dispose(bool disposing)
        {
            // Dispose only if we have not already disposed.
            if (!_disposed)
            {
                // If disposing equals true, dispose all managed and unmanaged resources.
                // I.e. dispose managed resources only if true, unmanaged always.
                if (disposing)
                {
                    NativeMemory.AlignedFree(_buckets);
                    _buckets = null;
                    NativeMemory.AlignedFree(_entries);
                    _entries = null;
                }
            }
            _disposed = true;
        }
    }
}