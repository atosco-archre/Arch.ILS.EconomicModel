
using System.Collections;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static System.Runtime.CompilerServices.Unsafe;

namespace Arch.EconomicModel.Benchmark
{
    [DebuggerDisplay("Size = {" + nameof(Size) + "}")]
    public unsafe class YearArray<TValue> : IDisposable, IEnumerable<TValue>
    {
        private const int Size = 10000;
        private readonly Entry[] _entries = new Entry[Size];
        private int _distinctYearCount = 0;

        [StructLayout(LayoutKind.Auto)]
        private struct Entry
        {
            public bool Set;
            public TValue value;
        }

        public int YearCount => _distinctYearCount;

        /// <summary>
        /// Clears the dictionary. Note that this invalidates any active enumerators.
        /// </summary>
        public void Clear()
        {
            Debug.Assert(_entries != null, "_entries should be non-null");
            Array.Clear(_entries);
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public ref TValue GetValueRefOrAddDefault(Int16Span index)
        {
            Entry[] entries = _entries;
            uint hashCode = (uint)index.GetHashCode();
            if(entries.GetAtUnsafe(hashCode).Set)
                return ref entries.GetAtUnsafe(hashCode).value!;
            return ref Add(hashCode);
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private ref TValue Add(uint hashCode)
        {
            ref Entry entry = ref _entries.GetAtUnsafe(hashCode);

            if (typeof(TValue) == typeof(SortedSet<RevoLayerYeltEntry>))
            {
                entry.value = (TValue)(object)new SortedSet<RevoLayerYeltEntry>(new RevoLayerEntryDayEventIdComparer());
            }
            else
            {
                entry.value = default!;
            }
            entry.Set = true;
            _distinctYearCount++;
            return ref entry.value!;
        }
        
        public Enumerator GetEnumerator() => new(this);
        IEnumerator<TValue> IEnumerable<TValue>.GetEnumerator() => new Enumerator(this);
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// Enumerator
        /// </summary>
        public struct Enumerator : IEnumerator<TValue>
        {
            private readonly YearArray<TValue> _arr;
            private int _index;
            private TValue _current;

            internal Enumerator(YearArray<TValue> arr)
            {
                _arr = arr;
                _index = 0;
                _current = default;
            }

            /// <summary>
            /// Move to next
            /// </summary>
            public bool MoveNext()
            {
                if(_index < Size)
                {
                    _current = _arr._entries[_index++].value;
                    return true;
                }
                else
                {
                    _current = default;
                    return false;
                }
            }

            /// <summary>
            /// Get current value
            /// </summary>
            public TValue Current => _current;

            object IEnumerator.Current => _current;

            void IEnumerator.Reset()
            {
                _index = 0;
                _current = default;
            }

            /// <summary>
            /// Dispose the enumerator
            /// </summary>
            public void Dispose()
            {
            }
        }

        private void ReleaseUnmanagedResources()
        {
        }

        public void Dispose()
        {
            ReleaseUnmanagedResources();
            GC.SuppressFinalize(this);
        }

        ~YearArray()
        {
            ReleaseUnmanagedResources();
        }
    }
}