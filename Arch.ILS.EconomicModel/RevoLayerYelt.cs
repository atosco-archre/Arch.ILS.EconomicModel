
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Arch.ILS.EconomicModel
{
    public unsafe class RevoLayerYelt : IYelt
    {
        public const int YEAR_COUNT = 10000;
        public const int YEAR_BUFFER_SIZE = YEAR_COUNT + 1;
        public const int BUFFER_ITEM_COUNT = 1024;
        public const int BUFFER_SIZE_BYTE = BUFFER_ITEM_COUNT;
        public const int BUFFER_SIZE_SHORT = BUFFER_ITEM_COUNT << 1;
        //public const int BUFFER_SIZE_INT = BUFFER_ITEM_COUNT << 2;
        public const int BUFFER_SIZE_DOUBLE = BUFFER_ITEM_COUNT << 3;
        public const int BUFFER_SIZE_LONG = BUFFER_ITEM_COUNT << 3;

        private readonly nint[] _yearDayPerilIdEventIdKeys;
        private readonly nint[] _days;
        private readonly nint[] _lossPcts;
        private readonly nint[] _RPs;
        private readonly nint[] _RBs;
        private readonly int _lastYearBufferIndex;
        private readonly int _lastBufferIndex;
        private int _lastBufferItemCount, _lastBufferSizeShort/*, _lastBufferSizeInt*/, _lastBufferSizeDouble, _lastBufferSizeLong;

        public RevoLayerYelt(in int lossAnalysisId, in int layerId, in IEnumerable<RevoLayerYeltEntry> yelt)
        {
            LossAnalysisId = lossAnalysisId;
            LayerId = layerId;
            Span<nint> yearRefs = stackalloc nint[YEAR_BUFFER_SIZE];
            var comparer = new RevoLayerEntryDayPerilIdEventIdComparer();
            int count = 0;
            foreach (var entry in yelt)
            {
                if (yearRefs[entry.Year] == nint.Zero)
                {
                    var newEntry = new SortedSet<RevoLayerYeltEntry>(comparer);
                    GCHandle handle = GCHandle.Alloc(newEntry);
                    yearRefs[entry.Year] = GCHandle.ToIntPtr(GCHandle.Alloc(newEntry));
                    if (!((SortedSet<RevoLayerYeltEntry>)handle.Target).Add(entry))
                        throw new Exception("Expected unique Year, Day, Event Peril");
                }
                else
                {
                    GCHandle handle = GCHandle.FromIntPtr(yearRefs[entry.Year]);
                    if (!((SortedSet<RevoLayerYeltEntry>)handle.Target).Add(entry))
                        throw new Exception("Expected unique Year, Day, Event Peril");
                }

                count++;
            }
            TotalEntryCount = count;
            yearRefs = yearRefs[1..];
            _lastBufferIndex = count / BUFFER_ITEM_COUNT;
            BufferCount = _lastBufferIndex + 1;
            _lastBufferItemCount = (count % BUFFER_ITEM_COUNT);
            _lastBufferSizeShort = _lastBufferItemCount << 1;
            //_lastBufferSizeInt = _lastBufferItemCount << 2;
            _lastBufferSizeDouble = _lastBufferItemCount << 3;
            _lastBufferSizeLong = _lastBufferSizeDouble;
            int currentBuffer = -1;
            int currentInBufferIndex = 0;

            _yearDayPerilIdEventIdKeys = new nint[BufferCount];
            _days = new nint[BufferCount];
            _lossPcts = new nint[BufferCount];
            _RPs = new nint[BufferCount];
            _RBs = new nint[BufferCount];
            ref nint spanStart = ref MemoryMarshal.GetReference(yearRefs);
            ref nint spanEnd = ref Unsafe.Add(ref spanStart, yearRefs.Length);
            Span<long> currentYearDayPerilIdEventIdBufferSpan = Span<long>.Empty;
            Span<short> currentDayBufferSpan = Span<short>.Empty;
            Span<double> currentLossPctBufferSpan = Span<double>.Empty;
            Span<double> currentRPBufferSpan = Span<double>.Empty;
            Span<double> currentRBBufferSpan = Span<double>.Empty;

            while (Unsafe.IsAddressLessThan(ref spanStart, ref spanEnd))
            {
                if (spanStart != nint.Zero)
                {
                    var yearEntries = (SortedSet<RevoLayerYeltEntry>)GCHandle.FromIntPtr(spanStart).Target;
                    foreach (var entry in yearEntries)
                    {
                        if ((currentInBufferIndex % BUFFER_ITEM_COUNT) == 0)
                        {
                            if (++currentBuffer == _lastBufferIndex)
                            {
                                _yearDayPerilIdEventIdKeys[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeLong);
                                _days[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeShort);
                                _lossPcts[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeDouble);
                                _RPs[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeDouble);
                                _RBs[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeDouble);

                                currentYearDayPerilIdEventIdBufferSpan = new Span<long>(_yearDayPerilIdEventIdKeys[currentBuffer].ToPointer(), _lastBufferItemCount);
                                currentDayBufferSpan = new Span<short>(_days[currentBuffer].ToPointer(), _lastBufferItemCount);
                                currentLossPctBufferSpan = new Span<double>(_lossPcts[currentBuffer].ToPointer(), _lastBufferItemCount);
                                currentRPBufferSpan = new Span<double>(_RPs[currentBuffer].ToPointer(), _lastBufferItemCount);
                                currentRBBufferSpan = new Span<double>(_RBs[currentBuffer].ToPointer(), _lastBufferItemCount);
                            }
                            else
                            {
                                _yearDayPerilIdEventIdKeys[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_LONG);
                                _days[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_SHORT);
                                _lossPcts[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_DOUBLE);
                                _RPs[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_DOUBLE);
                                _RBs[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_DOUBLE);

                                currentYearDayPerilIdEventIdBufferSpan = new Span<long>(_yearDayPerilIdEventIdKeys[currentBuffer].ToPointer(), BUFFER_ITEM_COUNT);
                                currentDayBufferSpan = new Span<short>(_days[currentBuffer].ToPointer(), BUFFER_ITEM_COUNT);
                                currentLossPctBufferSpan = new Span<double>(_lossPcts[currentBuffer].ToPointer(), BUFFER_ITEM_COUNT);
                                currentRPBufferSpan = new Span<double>(_RPs[currentBuffer].ToPointer(), BUFFER_ITEM_COUNT);
                                currentRBBufferSpan = new Span<double>(_RBs[currentBuffer].ToPointer(), BUFFER_ITEM_COUNT);
                            }

                            currentInBufferIndex = 0;
                        }

                        currentDayBufferSpan[currentInBufferIndex] = *entry.GetDay();
                        currentYearDayPerilIdEventIdBufferSpan[currentInBufferIndex] = (((long)(*entry.GetYear())) << 48) | (((long)(*entry.GetDay())) << 39) | (((long)(*entry.GetPerilId())) << 33) | (*entry.GetEventId());
                        currentLossPctBufferSpan[currentInBufferIndex] = *entry.GetLossPct();
                        currentRPBufferSpan[currentInBufferIndex] = *entry.GetRP();
                        currentRBBufferSpan[currentInBufferIndex] = *entry.GetRB();
                        currentInBufferIndex++;
                    }
                }

                spanStart = ref Unsafe.Add(ref spanStart, 1);
            }
        }
        public int LossAnalysisId { get; }
        public int LayerId { get; }
        public long RowVersion { get; }
        public int BufferCount { get; }
        public int TotalEntryCount { get; }
        public bool HasRP { get; }
        public bool HasRB { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<long> YearDayEventIdPerilIdKeys(in uint i) => new ReadOnlySpan<long>(_yearDayPerilIdEventIdKeys[i].ToPointer(), i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<short> Days(in uint i) => new ReadOnlySpan<short>(_days[i].ToPointer(), i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<double> LossPcts(in uint i) => new ReadOnlySpan<double>(_lossPcts[i].ToPointer(), i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<double> RPs(in uint i) => new ReadOnlySpan<double>(_RPs[i].ToPointer(), i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<double> RBs(in uint i) => new ReadOnlySpan<double>(_RBs[i].ToPointer(), i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        public void Dispose(bool disposing)
        {
            for (int i = 0; i < _yearDayPerilIdEventIdKeys.Length; i++)
            {
                Marshal.FreeHGlobal(_yearDayPerilIdEventIdKeys[i]);
                Marshal.FreeHGlobal(_days[i]);
                Marshal.FreeHGlobal(_lossPcts[i]);
                Marshal.FreeHGlobal(_RPs[i]);
                Marshal.FreeHGlobal(_RBs[i]);
                _yearDayPerilIdEventIdKeys[i] = nint.Zero;
                _days[i] = nint.Zero;
                _lossPcts[i] = nint.Zero;
                _RPs[i] = nint.Zero;
                _RBs[i] = nint.Zero;
            }
        }

        public void Dispose()
        {
        }
    }
}
