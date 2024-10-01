
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Arch.ILS.EconomicModel.Benchmark
{
    public unsafe class RevoLayerYeltStandardUnmanaged : IBenchmarkYelt, IDisposable
    {
        public const int YEAR_COUNT = 10_000;
        public const int YEAR_BUFFER_SIZE = YEAR_COUNT + 1;
        public const int BUFFER_ITEM_COUNT = 1024;
        public const int BUFFER_SIZE_BYTE = BUFFER_ITEM_COUNT;
        public const int BUFFER_SIZE_SHORT = BUFFER_ITEM_COUNT << 1;
        public const int BUFFER_SIZE_INT = BUFFER_ITEM_COUNT << 2;
        public const int BUFFER_SIZE_DOUBLE = BUFFER_ITEM_COUNT << 3;

        private readonly nint[] _distinctYears;
        private readonly nint[] _yearRepeatCount;
        private readonly nint[] _days;
        private readonly nint[] _eventIds;
        private readonly nint[] _lossPcts;
        private readonly nint[] _RPs;
        private readonly nint[] _RBs;
        private readonly int _lastYearBufferIndex;
        private readonly int _lastBufferIndex;
        private int _lastYearBufferItemCount, _lastYearBufferSizeShort, _lastYearBufferSizeInt;
        private int _lastBufferItemCount, _lastBufferSizeShort, _lastBufferSizeInt, _lastBufferSizeDouble;
        private bool _disposed;

        public RevoLayerYeltStandardUnmanaged(in int lossAnalysisId, in int layerId, in IEnumerable<RevoLayerYeltEntry> yelt)
        {
            _disposed = false;
            LossAnalysisId = lossAnalysisId;
            LayerId = layerId;
            SortedSet<RevoLayerYeltEntry>[] yearLosses = new SortedSet<RevoLayerYeltEntry>[YEAR_BUFFER_SIZE];
            Span<SortedSet<RevoLayerYeltEntry>> yearLossesSpan = yearLosses;
            int distinctYearCount = 0;
            int count = 0;
            var comparer = new RevoLayerEntryDayEventIdComparer();
            foreach (RevoLayerYeltEntry entry in yelt)
            {
                if (yearLossesSpan[entry.Year] == null)
                {
                    yearLossesSpan[entry.Year] = new SortedSet<RevoLayerYeltEntry> (comparer) { entry };
                    distinctYearCount++;
                }
                else
                    yearLossesSpan[entry.Year].Add(entry);
                count++;
            }

            yearLossesSpan = yearLossesSpan[1..];
            _lastYearBufferIndex = distinctYearCount / BUFFER_ITEM_COUNT;
            YearBufferCount = _lastYearBufferIndex + 1;
            _lastBufferIndex = count / BUFFER_ITEM_COUNT;
            BufferCount = _lastBufferIndex + 1;
            _lastYearBufferItemCount = (distinctYearCount % BUFFER_ITEM_COUNT); 
            _lastYearBufferSizeShort = _lastYearBufferItemCount << 1;
            _lastYearBufferSizeInt = _lastYearBufferItemCount << 2;
            _lastBufferItemCount = (count % BUFFER_ITEM_COUNT);
            _lastBufferSizeShort = _lastBufferItemCount << 1;
            _lastBufferSizeInt = _lastBufferItemCount << 2;
            _lastBufferSizeDouble = _lastBufferItemCount << 3;
            int currentYearBuffer = -1;
            int currentBuffer = -1;
            int currentYearInBufferIndex = 0;
            int currentInBufferIndex = 0;
            //int yearEndIndexExclusive = 0;

            _distinctYears = new nint[YearBufferCount];
            _yearRepeatCount = new nint[YearBufferCount];
            _days = new nint[BufferCount];
            _eventIds = new nint[BufferCount];
            _lossPcts = new nint[BufferCount];
            _RPs = new nint[BufferCount];
            _RBs = new nint[BufferCount];
            ref SortedSet<RevoLayerYeltEntry> spanStart = ref MemoryMarshal.GetReference(yearLossesSpan);
            ref SortedSet<RevoLayerYeltEntry> spanEnd = ref Unsafe.Add(ref spanStart, yearLossesSpan.Length);
            Span<short> currentYearBufferSpan = Span<short>.Empty;
            Span<int> currentYearRepeatCountBufferSpan = Span<int>.Empty;
            Span<short> currentDayBufferSpan = Span<short>.Empty;
            Span<int> currentEventIdBufferSpan = Span<int>.Empty;
            Span<double> currentLossPctBufferSpan = Span<double>.Empty;
            Span<double> currentRPBufferSpan = Span<double>.Empty;
            Span<double> currentRBBufferSpan = Span<double>.Empty;

            while (Unsafe.IsAddressLessThan(ref spanStart, ref spanEnd))
            {
                if (spanStart != null)
                {
                    foreach (var entry in spanStart)
                    {
                        if ((currentInBufferIndex % BUFFER_ITEM_COUNT) == 0)
                        {
                            if (++currentBuffer == _lastBufferIndex)
                            {
                                _days[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeShort);
                                _eventIds[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeInt);
                                _lossPcts[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeDouble);
                                _RPs[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeDouble);
                                _RBs[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeDouble);

                                currentDayBufferSpan = new Span<short>(_days[currentBuffer].ToPointer(), _lastBufferSizeShort);
                                currentEventIdBufferSpan = new Span<int>(_eventIds[currentBuffer].ToPointer(), _lastBufferSizeInt);
                                currentLossPctBufferSpan = new Span<double>(_lossPcts[currentBuffer].ToPointer(), _lastBufferSizeDouble);
                                currentRPBufferSpan = new Span<double>(_RPs[currentBuffer].ToPointer(), _lastBufferSizeDouble);
                                currentRBBufferSpan = new Span<double>(_RBs[currentBuffer].ToPointer(), _lastBufferSizeDouble);
                            }
                            else
                            {
                                _days[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_SHORT);
                                _eventIds[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_INT);
                                _lossPcts[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_DOUBLE);
                                _RPs[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_DOUBLE);
                                _RBs[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_DOUBLE);

                                currentDayBufferSpan = new Span<short>(_days[currentBuffer].ToPointer(), BUFFER_SIZE_SHORT);
                                currentEventIdBufferSpan = new Span<int>(_eventIds[currentBuffer].ToPointer(), BUFFER_SIZE_INT);
                                currentLossPctBufferSpan = new Span<double>(_lossPcts[currentBuffer].ToPointer(), BUFFER_SIZE_DOUBLE);
                                currentRPBufferSpan = new Span<double>(_RPs[currentBuffer].ToPointer(), BUFFER_SIZE_DOUBLE);
                                currentRBBufferSpan = new Span<double>(_RBs[currentBuffer].ToPointer(), BUFFER_SIZE_DOUBLE);
                            }

                            currentInBufferIndex = 0;
                        }

                        currentDayBufferSpan[currentInBufferIndex] = *entry.GetDay();
                        currentEventIdBufferSpan[currentInBufferIndex] = *entry.GetEventId();
                        currentLossPctBufferSpan[currentInBufferIndex] = *entry.GetLossPct();
                        currentRPBufferSpan[currentInBufferIndex] = *entry.GetRP();
                        currentRBBufferSpan[currentInBufferIndex] = *entry.GetRB();
                        currentInBufferIndex++;
                    }

                    //yearEndIndexExclusive += spanStart.Count;

                    if ((currentYearInBufferIndex % BUFFER_ITEM_COUNT) == 0)
                    {
                        if(++currentYearBuffer == _lastYearBufferIndex)
                        {
                            _distinctYears[currentYearBuffer] = Marshal.AllocHGlobal(_lastYearBufferSizeShort);
                            _yearRepeatCount[currentYearBuffer] = Marshal.AllocHGlobal(_lastYearBufferSizeInt);

                            currentYearBufferSpan = new Span<short>(_distinctYears[currentYearBuffer].ToPointer(), _lastYearBufferSizeShort);
                            currentYearRepeatCountBufferSpan = new Span<int>(_yearRepeatCount[currentYearBuffer].ToPointer(), _lastYearBufferSizeInt);
                        }
                        else
                        {
                            _distinctYears[currentYearBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_SHORT);
                            _yearRepeatCount[currentYearBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_INT);

                            currentYearBufferSpan = new Span<short>(_distinctYears[currentYearBuffer].ToPointer(), BUFFER_SIZE_SHORT);
                            currentYearRepeatCountBufferSpan = new Span<int>(_yearRepeatCount[currentYearBuffer].ToPointer(), BUFFER_SIZE_INT);
                        }

                        currentYearInBufferIndex = 0;
                    }

                    currentYearBufferSpan[currentYearInBufferIndex] = *spanStart.First().GetYear();
                    currentYearRepeatCountBufferSpan[currentYearInBufferIndex] = spanStart.Count;//yearEndIndexExclusive
                    currentYearInBufferIndex++;
                }

                spanStart = ref Unsafe.Add(ref spanStart, 1);
            }
        }
        public int LossAnalysisId { get; }
        public int LayerId { get; }

        public int YearBufferCount { get; }
        public int BufferCount { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<short> DistinctYears(in uint i) =>  new ReadOnlySpan<short>(_distinctYears[i].ToPointer(), i == _lastYearBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<int> YearRepeatCount(in uint i) =>  new ReadOnlySpan<int>(_yearRepeatCount[i].ToPointer(), i == _lastYearBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<short> Days(in uint i) => new ReadOnlySpan<short>(_days[i].ToPointer(), i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<int> EventIds(in uint i) => new ReadOnlySpan<int>(_eventIds[i].ToPointer(), i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<double> LossPcts(in uint i) => new ReadOnlySpan<double>(_lossPcts[i].ToPointer(), i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<double> RPs(in uint i) => new ReadOnlySpan<double>(_RPs[i].ToPointer(), i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<double> RBs(in uint i) => new ReadOnlySpan<double>(_RBs[i].ToPointer(), i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        public void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                }

                for (int i = 0; i < _distinctYears.Length; i++)
                {
                    Marshal.FreeHGlobal(_distinctYears[i]);
                    Marshal.FreeHGlobal(_yearRepeatCount[i]);
                    _distinctYears[i] = nint.Zero;
                    _yearRepeatCount[i] = nint.Zero;
                }

                for (int i = 0; i < _eventIds.Length; i++)
                {
                    Marshal.FreeHGlobal(_eventIds[i]);
                    Marshal.FreeHGlobal(_days[i]);
                    Marshal.FreeHGlobal(_lossPcts[i]);
                    Marshal.FreeHGlobal(_RPs[i]);
                    Marshal.FreeHGlobal(_RBs[i]);
                    _eventIds[i] = nint.Zero;
                    _days[i] = nint.Zero;
                    _lossPcts[i] = nint.Zero;
                    _RPs[i] = nint.Zero;
                    _RBs[i] = nint.Zero;
                }
            }

            _disposed = true;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
