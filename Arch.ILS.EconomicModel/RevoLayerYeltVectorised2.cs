
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Arch.ILS.EconomicModel
{
    public unsafe class RevoLayerYeltVectorised2 : IYelt, IDisposable
    {
        public const int YEAR_COUNT = 10000;
        public const int YEAR_BUFFER_SIZE = YEAR_COUNT + 1;
        public const int BUFFER_ITEM_COUNT = 1024;
        public const int BUFFER_SIZE_BYTE = BUFFER_ITEM_COUNT;
        public const int BUFFER_SIZE_SHORT = BUFFER_ITEM_COUNT << 1;
        //public const int BUFFER_SIZE_INT = BUFFER_ITEM_COUNT << 2;
        public const int BUFFER_SIZE_DOUBLE = BUFFER_ITEM_COUNT << 3;
        public const int BUFFER_SIZE_LONG = BUFFER_ITEM_COUNT << 3;

        private long** _yearDayPerilIdEventIdKeys;
        private short** _days;
        private double** _lossPcts;
        private double** _RPs;
        private double** _RBs;
        private readonly int _lastBufferIndex;
        private int _lastBufferItemCount, _lastBufferSizeShort/*, _lastBufferSizeInt*/, _lastBufferSizeDouble, _lastBufferSizeLong;
        private bool _disposed;

        public RevoLayerYeltVectorised2(in int lossAnalysisId, in int layerId, in IEnumerable<RevoLayerYeltEntry> yelt)
        {
            _disposed = false;
            if (!Avx.IsSupported && !Avx2.IsSupported)
                throw new Exception("AVX and AVX2 not supported on this machine. Please use the non vectorised version of this class.");
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
            _lastBufferItemCount = (int)(count % BUFFER_ITEM_COUNT);
            _lastBufferSizeShort = _lastBufferItemCount << 1;
            //_lastBufferSizeInt = _lastBufferItemCount << 2;
            _lastBufferSizeDouble = _lastBufferItemCount << 3;
            _lastBufferSizeLong = _lastBufferSizeDouble;
            int currentBuffer = -1;
            int currentInBufferIndex = 0;

            int vectorLongCount = Avx2.IsSupported ? Vector256<long>.Count : Vector128<long>.Count;
            short* tempYears = (short*)NativeMemory.AlignedAlloc((nuint)(vectorLongCount << 1), (nuint)Unsafe.SizeOf<short>());
            short* tempCurrentYear = tempYears;
            short* tempLastYear = tempYears + vectorLongCount;
            short* tempDays = (short*)NativeMemory.AlignedAlloc((nuint)(vectorLongCount << 1), (nuint)Unsafe.SizeOf<short>());
            short* tempCurrentDay = tempDays;
            long* tempEventIds = (long*)NativeMemory.AlignedAlloc((nuint)(vectorLongCount << 3), (nuint)Unsafe.SizeOf<long>());
            long* tempCurrentEventId = tempEventIds;
            nint tempPerilIdAlloc = Marshal.AllocHGlobal(vectorLongCount);
            byte* tempPerilIds = (byte*)tempPerilIdAlloc.ToPointer();
            byte* tempCurrentPerilId = tempPerilIds;

            nuint ptrSize = (nuint)(Unsafe.SizeOf<IntPtr>() * BufferCount);
            _yearDayPerilIdEventIdKeys = (long**)NativeMemory.AlignedAlloc(ptrSize, (nuint)Unsafe.SizeOf<IntPtr>());
            _days = (short**)NativeMemory.AlignedAlloc(ptrSize, (nuint)Unsafe.SizeOf<IntPtr>());
            _lossPcts = (double**)NativeMemory.AlignedAlloc(ptrSize, (nuint)Unsafe.SizeOf<IntPtr>());
            _RPs = (double**)NativeMemory.AlignedAlloc(ptrSize, (nuint)Unsafe.SizeOf<IntPtr>());
            _RBs = (double**)NativeMemory.AlignedAlloc(ptrSize, (nuint)Unsafe.SizeOf<IntPtr>());
            ref nint spanStart = ref MemoryMarshal.GetReference(yearRefs);
            ref nint spanEnd = ref Unsafe.Add(ref spanStart, yearRefs.Length);
            long* currentYearDayPerilIdEventIdPtr = null;
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
                                _yearDayPerilIdEventIdKeys[currentBuffer] = (long*)NativeMemory.AlignedAlloc((nuint)_lastBufferSizeLong, sizeof(long));
                                _days[currentBuffer] = (short*)NativeMemory.AlignedAlloc((nuint)_lastBufferSizeShort, sizeof(short));
                                _lossPcts[currentBuffer] = (double*)NativeMemory.AlignedAlloc((nuint)_lastBufferSizeDouble, sizeof(double));
                                _RPs[currentBuffer] = (double*)NativeMemory.AlignedAlloc((nuint)_lastBufferSizeDouble, sizeof(double));
                                _RBs[currentBuffer] = (double*)NativeMemory.AlignedAlloc((nuint)_lastBufferSizeDouble, sizeof(double));

                                currentYearDayPerilIdEventIdPtr = _yearDayPerilIdEventIdKeys[currentBuffer];
                                currentDayBufferSpan = new Span<short>(_days[currentBuffer], _lastBufferItemCount);
                                currentLossPctBufferSpan = new Span<double>(_lossPcts[currentBuffer], _lastBufferItemCount);
                                currentRPBufferSpan = new Span<double>(_RPs[currentBuffer], _lastBufferItemCount);
                                currentRBBufferSpan = new Span<double>(_RBs[currentBuffer], _lastBufferItemCount);
                            }
                            else
                            {
                                _yearDayPerilIdEventIdKeys[currentBuffer] = (long*)NativeMemory.AlignedAlloc(BUFFER_SIZE_LONG, sizeof(long));
                                _days[currentBuffer] = (short*)NativeMemory.AlignedAlloc(BUFFER_SIZE_SHORT, sizeof(short));
                                _lossPcts[currentBuffer] = (double*)NativeMemory.AlignedAlloc(BUFFER_SIZE_DOUBLE, sizeof(double));
                                _RPs[currentBuffer] = (double*)NativeMemory.AlignedAlloc(BUFFER_SIZE_DOUBLE, sizeof(double));
                                _RBs[currentBuffer] = (double*)NativeMemory.AlignedAlloc(BUFFER_SIZE_DOUBLE, sizeof(double));

                                currentYearDayPerilIdEventIdPtr = _yearDayPerilIdEventIdKeys[currentBuffer];
                                currentDayBufferSpan = new Span<short>(_days[currentBuffer], BUFFER_ITEM_COUNT);
                                currentLossPctBufferSpan = new Span<double>(_lossPcts[currentBuffer], BUFFER_ITEM_COUNT);
                                currentRPBufferSpan = new Span<double>(_RPs[currentBuffer], BUFFER_ITEM_COUNT);
                                currentRBBufferSpan = new Span<double>(_RBs[currentBuffer], BUFFER_ITEM_COUNT);
                            }

                            currentInBufferIndex = 0;
                        }

                        *tempCurrentYear++ = *entry.GetYear();
                        *tempCurrentDay++ = *entry.GetDay();
                        *tempCurrentPerilId++ = *entry.GetPerilId();
                        *tempCurrentEventId++ = *entry.GetEventId();
                        currentDayBufferSpan[currentInBufferIndex] = *entry.GetDay();                        
                        currentLossPctBufferSpan[currentInBufferIndex] = *entry.GetLossPct();
                        currentRPBufferSpan[currentInBufferIndex] = *entry.GetRP();
                        currentRBBufferSpan[currentInBufferIndex] = *entry.GetRB();

                        if(tempCurrentYear == tempLastYear)
                        {
                            tempCurrentYear = tempYears;
                            tempCurrentDay = tempDays;
                            tempCurrentPerilId = tempPerilIds;
                            tempCurrentEventId = tempEventIds;
                            //the key is identical using long instead of ulong for year in [1, 10000], day in [1, 365], perilId in [0, 63] and EventId in [1, 8589934591]. 
                            // (((ulong)(ushort)(short)10000)<<48)|(((ulong)(ushort)(short)365)<<39)|(((ulong)(byte)255)<<33)|((ulong)(uint)Int32.MaxValue) = (((long)(short)10000)<<48)|(((long)(short)365)<<39)|(((long)(byte)255)<<33)|((long)Int32.MaxValue) = 2814952070803619839

                            if (Avx2.IsSupported)
                            {
                                var key = ((Avx2.ConvertToVector256Int64(tempCurrentYear)) << 48) | ((Avx2.ConvertToVector256Int64(tempCurrentDay)) << 39) | ((Avx2.ConvertToVector256Int64(tempCurrentPerilId)) << 33) | Avx2.LoadVector256(tempCurrentEventId);
                                Avx2.Store(currentYearDayPerilIdEventIdPtr, key);
                                currentYearDayPerilIdEventIdPtr += Vector256<long>.Count;
                            }
                            else if (Avx.IsSupported)
                            {
                                var key = ((Avx.ConvertToVector128Int64(tempCurrentYear)) << 48) | ((Avx.ConvertToVector128Int64(tempCurrentDay)) << 39) | ((Avx.ConvertToVector128Int64(tempCurrentPerilId)) << 33) | Avx.LoadVector128(tempCurrentEventId);
                                Avx.Store(currentYearDayPerilIdEventIdPtr, key);
                                currentYearDayPerilIdEventIdPtr += Vector128<long>.Count;
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
                tempCurrentPerilId = tempPerilIds;
                tempCurrentEventId = tempEventIds;
                while(tempCurrentYear < tempLastYear)
                {
                    //the key is identical using long instead of ulong for year in [1, 10000], day in [1, 365], perilId in [0, 63] and EventId in [1, 8589934591]. 
                    *currentYearDayPerilIdEventIdPtr++ = (((long)*tempCurrentYear++) << 48) | (((long)*tempCurrentDay++) << 39) | (((long)*tempCurrentPerilId++) << 33) | *tempCurrentEventId++;
                }
            }
        }

        public int LossAnalysisId { get; }
        public int LayerId { get; }
        public long RowVersion { get; set; }
        public int BufferCount { get; }
        public int TotalEntryCount { get; }
        public bool HasRP { get; }
        public bool HasRB { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<long> YearDayEventIdPerilIdKeys(in uint i) => new ReadOnlySpan<long>(_yearDayPerilIdEventIdKeys[i], i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<short> Days(in uint i) => new ReadOnlySpan<short>(_days[i], i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<double> LossPcts(in uint i) => new ReadOnlySpan<double>(_lossPcts[i], i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<double> RPs(in uint i) => new ReadOnlySpan<double>(_RPs[i], i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<double> RBs(in uint i) => new ReadOnlySpan<double>(_RBs[i], i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        public void Dispose(bool disposing)
        {
            if(!_disposed)
            {
                if(disposing)
                {
                }

                for (int i = 0; i < BufferCount; i++)
                {
                    NativeMemory.AlignedFree(_yearDayPerilIdEventIdKeys[i]);
                    NativeMemory.AlignedFree(_days[i]);
                    NativeMemory.AlignedFree(_lossPcts[i]);
                    NativeMemory.AlignedFree(_RPs[i]);
                    NativeMemory.AlignedFree(_RBs[i]);
                    _yearDayPerilIdEventIdKeys[i] = null;
                    _days[i] = null;
                    _lossPcts[i] = null;
                    _RPs[i] = null;
                    _RBs[i] = null;
                }

                NativeMemory.AlignedFree(_yearDayPerilIdEventIdKeys);
                NativeMemory.AlignedFree(_days);
                NativeMemory.AlignedFree(_lossPcts);
                NativeMemory.AlignedFree(_RPs);
                NativeMemory.AlignedFree(_RBs);
                _yearDayPerilIdEventIdKeys = null;
                _days = null;
                _lossPcts = null;
                _RPs = null;
                _RBs = null;
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
