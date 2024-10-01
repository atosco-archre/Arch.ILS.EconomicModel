
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Arch.ILS.EconomicModel.Benchmark
{
    public unsafe class RevoLayerYeltYearArrayUnsafe : IBenchmarkYelt, IDisposable
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

        public RevoLayerYeltYearArrayUnsafe(in int lossAnalysisId, in int layerId, in IEnumerable<RevoLayerYeltEntry> yelt)
        {
            _disposed = false;
            LossAnalysisId = lossAnalysisId;
            LayerId = layerId;
            YearArray<SortedSet<RevoLayerYeltEntry>> yearArray = new();
            int count = 0;
            foreach(var entry in yelt)
            {
                ref SortedSet<RevoLayerYeltEntry> s = ref yearArray.GetValueRefOrAddDefault(new Int16Span(entry.GetYear()));
                s.Add(entry);
                count++;
            }

            _lastYearBufferIndex = yearArray.YearCount / BUFFER_ITEM_COUNT;
            YearBufferCount = _lastYearBufferIndex + 1;
            _lastBufferIndex = count / BUFFER_ITEM_COUNT;
            BufferCount = _lastBufferIndex + 1;
            _lastYearBufferItemCount = (yearArray.YearCount % BUFFER_ITEM_COUNT);
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
            short* distinctYears = (short*)nint.Zero.ToPointer();
            _yearRepeatCount = new nint[YearBufferCount];
            int* yearRepeatCount = (int*)nint.Zero.ToPointer();
            _days = new nint[BufferCount];
            short* days = (short*)nint.Zero.ToPointer();
            _eventIds = new nint[BufferCount];
            int* eventIds = (int*)nint.Zero.ToPointer();
            _lossPcts = new nint[BufferCount];
            double* lossPcts = (double*)nint.Zero.ToPointer();
            _RPs = new nint[BufferCount];
            double* rps = (double*)nint.Zero.ToPointer();
            _RBs = new nint[BufferCount];
            double* rbs = (double*)nint.Zero.ToPointer();

            foreach (SortedSet<RevoLayerYeltEntry> yearEntry in yearArray)
            {
                if (yearEntry != null)
                {
                    foreach (var entry in yearEntry)
                    {
                        if ((currentInBufferIndex % BUFFER_ITEM_COUNT) == 0)
                        {
                            ++currentBuffer;

                            if (currentBuffer == _lastBufferIndex)
                            {
                                _days[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeShort);
                                _eventIds[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeInt);
                                _lossPcts[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeDouble);
                                _RPs[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeDouble);
                                _RBs[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeDouble);
                            }
                            else
                            {
                                _days[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_SHORT);
                                _eventIds[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_INT);
                                _lossPcts[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_DOUBLE);
                                _RPs[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_DOUBLE);
                                _RBs[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_DOUBLE);
                            }

                            days = (short*)_days[currentBuffer].ToPointer();
                            eventIds = (int*)_eventIds[currentBuffer].ToPointer();
                            lossPcts = (double*)_lossPcts[currentBuffer].ToPointer();
                            rps = (double*)_RPs[currentBuffer].ToPointer();
                            rbs = (double*)_RBs[currentBuffer].ToPointer();

                            currentInBufferIndex = 0;
                        }

                        *days++ = *entry.GetDay();
                        *eventIds++ = *entry.GetEventId();
                        *lossPcts++ = *entry.GetLossPct();
                        *rps++ = *entry.GetRP();
                        *rbs++ = *entry.GetRB();
                        currentInBufferIndex++;
                    }

                    //yearEndIndexExclusive += spanStart.Count;

                    if ((currentYearInBufferIndex % BUFFER_ITEM_COUNT) == 0)
                    {
                        ++currentYearBuffer;

                        if (currentYearBuffer == _lastYearBufferIndex)
                        {
                            _distinctYears[currentYearBuffer] = Marshal.AllocHGlobal(_lastYearBufferSizeShort);
                            _yearRepeatCount[currentYearBuffer] = Marshal.AllocHGlobal(_lastYearBufferSizeInt);
                        }
                        else
                        {
                            _distinctYears[currentYearBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_SHORT);
                            _yearRepeatCount[currentYearBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_INT);
                        }

                        distinctYears = (short*)_distinctYears[currentYearBuffer].ToPointer();
                        yearRepeatCount = (int*)_yearRepeatCount[currentYearBuffer].ToPointer();

                        currentYearInBufferIndex = 0;
                    }

                    *distinctYears++ = *yearEntry.First().GetYear();
                    *yearRepeatCount++ = yearEntry.Count;//yearEndIndexExclusive
                    currentYearInBufferIndex++;
                }
            }
        }

        public int LossAnalysisId { get; }
        public int LayerId { get; }
        public int YearBufferCount { get; }
        public int BufferCount { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<short> DistinctYears(in uint i) => new ReadOnlySpan<short>(_distinctYears[i].ToPointer(), i == _lastYearBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<int> YearRepeatCount(in uint i) => new ReadOnlySpan<int>(_yearRepeatCount[i].ToPointer(), i == _lastYearBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

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
            if(!_disposed)
            {
                if(disposing)
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
