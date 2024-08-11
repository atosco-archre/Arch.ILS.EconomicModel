
using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Arch.ILS.EconomicModel
{
    public unsafe class RevoLayerDayYeltVectorised : IYelt
    {
        public const int DAY_COUNT = 365;
        public const int DAY_BUFFER_SIZE = DAY_COUNT + 1;
        public const int BUFFER_ITEM_COUNT = 1024;
        public const int BUFFER_SIZE_BYTE = BUFFER_ITEM_COUNT;
        public const int BUFFER_SIZE_SHORT = BUFFER_ITEM_COUNT << 1;
        public const int BUFFER_SIZE_INT = BUFFER_ITEM_COUNT << 2;
        public const int BUFFER_SIZE_DOUBLE = BUFFER_ITEM_COUNT << 3;
        public const int BUFFER_SIZE_LONG = BUFFER_ITEM_COUNT << 3;

        private readonly nint[] _dayYearEventIdKeys;
        private readonly nint[] _days;
        private readonly nint[] _lossPcts;
        private readonly nint[] _RPs;
        private readonly nint[] _RBs;
        private readonly int _lastBufferIndex;
        private int _lastBufferItemCount, _lastBufferSizeShort, _lastBufferSizeInt, _lastBufferSizeDouble, _lastBufferSizeLong;

        public RevoLayerDayYeltVectorised(in int lossAnalysisId, in int layerId, in IEnumerable<RevoLayerYeltEntry> yelt)
        {
            if (!Avx.IsSupported && !Avx2.IsSupported)
                throw new Exception("AVX and AVX2 not supported on this machine. Please use the non vectorised version of this class.");
            LossAnalysisId = lossAnalysisId;
            LayerId = layerId;
            Span<nint> dayRefs = stackalloc nint[DAY_BUFFER_SIZE];
            var comparer = new RevoLayerEntryYearEventIdComparer();
            int count = 0;
            foreach (var entry in yelt)
            {
                if (dayRefs[entry.Day] == nint.Zero)
                {
                    var newEntry = new SortedSet<RevoLayerYeltEntry>(comparer);
                    GCHandle handle = GCHandle.Alloc(newEntry);
                    dayRefs[entry.Day] = GCHandle.ToIntPtr(GCHandle.Alloc(newEntry));
                    ((SortedSet<RevoLayerYeltEntry>)handle.Target).Add(entry);
                }
                else
                {
                    GCHandle handle = GCHandle.FromIntPtr(dayRefs[entry.Day]);
                    ((SortedSet<RevoLayerYeltEntry>)handle.Target).Add(entry);
                }
                count++;
            }
            TotalEntryCount = count;
            dayRefs = dayRefs[1..];
            _lastBufferIndex = count / BUFFER_ITEM_COUNT;
            BufferCount = _lastBufferIndex + 1;
            _lastBufferItemCount = (count % BUFFER_ITEM_COUNT);
            _lastBufferSizeShort = _lastBufferItemCount << 1;
            _lastBufferSizeInt = _lastBufferItemCount << 2;
            _lastBufferSizeDouble = _lastBufferItemCount << 3;
            _lastBufferSizeLong = _lastBufferSizeDouble;
            int currentBuffer = -1;
            int currentInBufferIndex = 0;

            int vectorLongCount = Avx2.IsSupported ? Vector256<long>.Count : Vector128<long>.Count;
            nint tempYearAlloc = Marshal.AllocHGlobal(vectorLongCount << 1);
            short* tempYears = (short*)tempYearAlloc.ToPointer();
            short* tempCurrentYear = tempYears;
            short* tempLastYear = tempYears + vectorLongCount;
            nint tempDayAlloc = Marshal.AllocHGlobal(vectorLongCount << 1);
            short* tempDays = (short*)tempDayAlloc.ToPointer();
            short* tempCurrentDay = tempDays;
            nint tempEventIdAlloc = Marshal.AllocHGlobal(vectorLongCount << 2);
            int* tempEventIds = (int*)tempEventIdAlloc.ToPointer();
            int* tempCurrentEventId = tempEventIds;

            _dayYearEventIdKeys = new nint[BufferCount];
            _days = new nint[BufferCount];
            _lossPcts = new nint[BufferCount];
            _RPs = new nint[BufferCount];
            _RBs = new nint[BufferCount];
            ref nint spanStart = ref MemoryMarshal.GetReference(dayRefs);
            ref nint spanEnd = ref Unsafe.Add(ref spanStart, dayRefs.Length);
            long* currentYearDayEventIdPtr = null;
            Span<short> currentDayBufferSpan = Span<short>.Empty;
            Span<double> currentLossPctBufferSpan = Span<double>.Empty;
            Span<double> currentRPBufferSpan = Span<double>.Empty;
            Span<double> currentRBBufferSpan = Span<double>.Empty;

            while (Unsafe.IsAddressLessThan(ref spanStart, ref spanEnd))
            {
                if (spanStart != nint.Zero)
                {
                    var dayEntries = (SortedSet<RevoLayerYeltEntry>)GCHandle.FromIntPtr(spanStart).Target;
                    foreach (var entry in dayEntries)
                    {
                        if ((currentInBufferIndex % BUFFER_ITEM_COUNT) == 0)
                        {
                            if (++currentBuffer == _lastBufferIndex)
                            {
                                _dayYearEventIdKeys[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeLong);
                                _days[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeShort);                                
                                _lossPcts[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeDouble);
                                _RPs[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeDouble);
                                _RBs[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeDouble);

                                currentYearDayEventIdPtr = (long*)_dayYearEventIdKeys[currentBuffer].ToPointer();
                                currentDayBufferSpan = new Span<short>(_days[currentBuffer].ToPointer(), _lastBufferSizeShort);
                                currentLossPctBufferSpan = new Span<double>(_lossPcts[currentBuffer].ToPointer(), _lastBufferSizeDouble);
                                currentRPBufferSpan = new Span<double>(_RPs[currentBuffer].ToPointer(), _lastBufferSizeDouble);
                                currentRBBufferSpan = new Span<double>(_RBs[currentBuffer].ToPointer(), _lastBufferSizeDouble);
                            }
                            else
                            {
                                _dayYearEventIdKeys[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_LONG); 
                                _days[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_SHORT);
                                _lossPcts[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_DOUBLE);
                                _RPs[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_DOUBLE);
                                _RBs[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_DOUBLE);

                                currentYearDayEventIdPtr = (long*)_dayYearEventIdKeys[currentBuffer].ToPointer();
                                currentDayBufferSpan = new Span<short>(_days[currentBuffer].ToPointer(), BUFFER_SIZE_SHORT);
                                currentLossPctBufferSpan = new Span<double>(_lossPcts[currentBuffer].ToPointer(), BUFFER_SIZE_DOUBLE);
                                currentRPBufferSpan = new Span<double>(_RPs[currentBuffer].ToPointer(), BUFFER_SIZE_DOUBLE);
                                currentRBBufferSpan = new Span<double>(_RBs[currentBuffer].ToPointer(), BUFFER_SIZE_DOUBLE);
                            }

                            currentInBufferIndex = 0;
                        }

                        *tempCurrentYear++ = *entry.GetYear();
                        *tempCurrentDay++ = *entry.GetDay();
                        *tempCurrentEventId++ = *entry.GetEventId();
                        currentDayBufferSpan[currentInBufferIndex] = *entry.GetDay();                        
                        currentLossPctBufferSpan[currentInBufferIndex] = *entry.GetLossPct();
                        currentRPBufferSpan[currentInBufferIndex] = *entry.GetRP();
                        currentRBBufferSpan[currentInBufferIndex] = *entry.GetRB();

                        if(tempCurrentYear == tempLastYear)
                        {
                            tempCurrentYear = tempYears;
                            tempCurrentDay = tempDays;
                            tempCurrentEventId = tempEventIds;
                            //the key is identical using long instead of ulong for day in [1, 365], year in [0, 10000] and EventId in [1, Int32.MaxValue]. 
                            // (((ulong)(ushort)(short)365)<<48)|(((ulong)(ushort)(short)10000)<<32)|((ulong)(uint)Int32.MaxValue) = (((long)(short)365)<<48)|(((long)(short)10000)<<32)|((long)Int32.MaxValue) = 102781318319833087

                            if (Avx2.IsSupported)
                            {
                                var key = ((Avx2.ConvertToVector256Int64(tempCurrentDay)) << 48) | ((Avx2.ConvertToVector256Int64(tempCurrentYear)) << 32) | Avx2.ConvertToVector256Int64(tempCurrentEventId);
                                Avx2.Store(currentYearDayEventIdPtr, key);
                                currentYearDayEventIdPtr += Vector256<long>.Count;
                            }
                            else if (Avx.IsSupported)
                            {
                                var key = ((Avx.ConvertToVector128Int64(tempCurrentDay)) << 48) | ((Avx.ConvertToVector128Int64(tempCurrentYear)) << 32) | Avx.ConvertToVector128Int64(tempCurrentEventId);
                                Avx.Store(currentYearDayEventIdPtr, key);
                                currentYearDayEventIdPtr += Vector128<long>.Count;
                            }
                        }

                        currentInBufferIndex++;
                    }
                }

                spanStart = ref Unsafe.Add(ref spanStart, 1);
            }

            if (tempCurrentYear != tempLastYear)
            {
                tempLastYear = tempCurrentYear;
                tempCurrentYear = tempYears;
                tempCurrentDay = tempDays;
                tempCurrentEventId = tempEventIds;
                while(tempCurrentYear < tempLastYear)
                {
                    //the key is identical using long instead of ulong for day in [1, 365], year in [0, 10000] and EventId in [1, Int32.MaxValue]. 
                    *currentYearDayEventIdPtr++ = (((long)*tempCurrentDay++) << 48) | (((long)*tempCurrentYear++) << 32) | *tempCurrentEventId++;
                }
            }
        }

        public int LossAnalysisId { get; }
        public int LayerId { get; }
        public int BufferCount { get; }

        public int TotalEntryCount { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<long> YearDayEventIdKeys(in uint i) => new ReadOnlySpan<long>(_dayYearEventIdKeys[i].ToPointer(), i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

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
            for (int i = 0; i < _dayYearEventIdKeys.Length; i++)
            {
                Marshal.FreeHGlobal(_dayYearEventIdKeys[i]);
                Marshal.FreeHGlobal(_days[i]);
                Marshal.FreeHGlobal(_lossPcts[i]);
                Marshal.FreeHGlobal(_RPs[i]);
                Marshal.FreeHGlobal(_RBs[i]);
                _dayYearEventIdKeys[i] = nint.Zero;
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
