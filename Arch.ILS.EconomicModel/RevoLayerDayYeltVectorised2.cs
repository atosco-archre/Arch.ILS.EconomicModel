
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

using Arch.ILS.EconomicModel.Binary;

namespace Arch.ILS.EconomicModel
{
    public unsafe class RevoLayerDayYeltVectorised2 : IYelt, IDisposable
    {
        public const int DAY_COUNT = 365;
        public const int DAY_BUFFER_SIZE = DAY_COUNT + 1;
        public const int BUFFER_ITEM_COUNT = 1024;
        public const int BUFFER_SIZE_BYTE = BUFFER_ITEM_COUNT;
        public const int BUFFER_SIZE_SHORT = BUFFER_ITEM_COUNT << 1;
        public const int BUFFER_SIZE_INT = BUFFER_ITEM_COUNT << 2;
        public const int BUFFER_SIZE_DOUBLE = BUFFER_ITEM_COUNT << 3;
        public const int BUFFER_SIZE_LONG = BUFFER_ITEM_COUNT << 3;

        private long** _dayYearPerilIdEventIdKeys;
        private short** _days;
        private double** _lossPcts;
        private double** _RPs;
        private double** _RBs;
        private readonly int _lastBufferIndex;
        private int _lastBufferItemCount, _lastBufferSizeShort, _lastBufferSizeInt, _lastBufferSizeDouble, _lastBufferSizeLong;
        private bool _disposed;

        public RevoLayerDayYeltVectorised2(in int lossAnalysisId, in int layerId, in IEnumerable<RevoLayerYeltEntry> yelt)
        {
            _disposed = false;
            if (!Avx.IsSupported && !Avx2.IsSupported)
                throw new Exception("AVX and AVX2 not supported on this machine. Please use the non vectorised version of this class.");
            LossAnalysisId = lossAnalysisId;
            LayerId = layerId;
            Span<nint> dayRefs = stackalloc nint[DAY_BUFFER_SIZE];
            var comparer = new RevoLayerEntryYearPerilIdEventIdComparer();
            int count = 0;
            foreach (var entry in yelt)
            {
                if (dayRefs[entry.Day] == nint.Zero)
                {
                    var newEntry = new SortedSet<RevoLayerYeltEntry>(comparer);
                    GCHandle handle = GCHandle.Alloc(newEntry);
                    dayRefs[entry.Day] = GCHandle.ToIntPtr(GCHandle.Alloc(newEntry));
                    if(!((SortedSet<RevoLayerYeltEntry>)handle.Target).Add(entry))
                        throw new Exception("Expected unique Year, Day, Event Peril");
                }
                else
                {
                    GCHandle handle = GCHandle.FromIntPtr(dayRefs[entry.Day]);
                    if (!((SortedSet<RevoLayerYeltEntry>)handle.Target).Add(entry))
                        throw new Exception("Expected unique Year, Day, Event Peril");
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
            nint tempPerilIdAlloc = Marshal.AllocHGlobal(vectorLongCount);
            byte* tempPerilIds = (byte*)tempPerilIdAlloc.ToPointer();
            byte* tempCurrentPerilId = tempPerilIds;

            nuint ptrSize = (nuint)(Unsafe.SizeOf<IntPtr>() * BufferCount);
            _dayYearPerilIdEventIdKeys = (long**)NativeMemory.AlignedAlloc(ptrSize, (nuint)Unsafe.SizeOf<IntPtr>());
            _days = (short**)NativeMemory.AlignedAlloc(ptrSize, (nuint)Unsafe.SizeOf<IntPtr>());
            _lossPcts = (double**)NativeMemory.AlignedAlloc(ptrSize, (nuint)Unsafe.SizeOf<IntPtr>());
            _RPs = (double**)NativeMemory.AlignedAlloc(ptrSize, (nuint)Unsafe.SizeOf<IntPtr>());
            _RBs = (double**)NativeMemory.AlignedAlloc(ptrSize, (nuint)Unsafe.SizeOf<IntPtr>());
            ref nint spanStart = ref MemoryMarshal.GetReference(dayRefs);
            ref nint spanEnd = ref Unsafe.Add(ref spanStart, dayRefs.Length);
            long* currentYearDayPerilIdEventIdPtr = null;
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
                                _dayYearPerilIdEventIdKeys[currentBuffer] = (long*)NativeMemory.AlignedAlloc((nuint)_lastBufferSizeLong, sizeof(long));
                                _days[currentBuffer] = (short*)NativeMemory.AlignedAlloc((nuint)_lastBufferSizeShort, sizeof(short));
                                _lossPcts[currentBuffer] = (double*)NativeMemory.AlignedAlloc((nuint)_lastBufferSizeDouble, sizeof(double));
                                _RPs[currentBuffer] = (double*)NativeMemory.AlignedAlloc((nuint)_lastBufferSizeDouble, sizeof(double));
                                _RBs[currentBuffer] = (double*)NativeMemory.AlignedAlloc((nuint)_lastBufferSizeDouble, sizeof(double));

                                currentYearDayPerilIdEventIdPtr = _dayYearPerilIdEventIdKeys[currentBuffer];
                                currentDayBufferSpan = new Span<short>(_days[currentBuffer], _lastBufferSizeShort);
                                currentLossPctBufferSpan = new Span<double>(_lossPcts[currentBuffer], _lastBufferSizeDouble);
                                currentRPBufferSpan = new Span<double>(_RPs[currentBuffer], _lastBufferSizeDouble);
                                currentRBBufferSpan = new Span<double>(_RBs[currentBuffer], _lastBufferSizeDouble);
                            }
                            else
                            {
                                _dayYearPerilIdEventIdKeys[currentBuffer] = (long*)NativeMemory.AlignedAlloc(BUFFER_SIZE_LONG, sizeof(long));
                                _days[currentBuffer] = (short*)NativeMemory.AlignedAlloc(BUFFER_SIZE_SHORT, sizeof(short));
                                _lossPcts[currentBuffer] = (double*)NativeMemory.AlignedAlloc(BUFFER_SIZE_DOUBLE, sizeof(double));
                                _RPs[currentBuffer] = (double*)NativeMemory.AlignedAlloc(BUFFER_SIZE_DOUBLE, sizeof(double));
                                _RBs[currentBuffer] = (double*)NativeMemory.AlignedAlloc(BUFFER_SIZE_DOUBLE, sizeof(double));

                                currentYearDayPerilIdEventIdPtr = _dayYearPerilIdEventIdKeys[currentBuffer];
                                currentDayBufferSpan = new Span<short>(_days[currentBuffer], BUFFER_SIZE_SHORT);
                                currentLossPctBufferSpan = new Span<double>(_lossPcts[currentBuffer], BUFFER_SIZE_DOUBLE);
                                currentRPBufferSpan = new Span<double>(_RPs[currentBuffer], BUFFER_SIZE_DOUBLE);
                                currentRBBufferSpan = new Span<double>(_RBs[currentBuffer], BUFFER_SIZE_DOUBLE);
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

                        if (tempCurrentYear == tempLastYear)
                        {
                            tempCurrentYear = tempYears;
                            tempCurrentDay = tempDays;
                            tempCurrentPerilId = tempPerilIds;
                            tempCurrentEventId = tempEventIds;
                            //the key is identical using long instead of ulong for day in [1, 365], year in [0, 10000], perilId in [0, 255] and EventId in [1, Int32.MaxValue]. 
                            //(((ulong)(ushort)(short)365)<<49)|(((ulong)(ushort)(short)10000)<<33)| (((ulong)(byte)255) << 32)| ((ulong)(uint)Int32.MaxValue) = (((long)(short)365)<<49)|(((long)(short)10000)<<33) | (((long)(byte)255) << 32) | ((long)Int32.MaxValue) = 205563592269889535

                            if (Avx2.IsSupported)
                            {
                                var key = ((Avx2.ConvertToVector256Int64(tempCurrentDay)) << 49) | ((Avx2.ConvertToVector256Int64(tempCurrentYear)) << 33) | ((Avx2.ConvertToVector256Int64(tempCurrentPerilId)) << 32) | Avx2.ConvertToVector256Int64(tempCurrentEventId);
                                Avx2.Store(currentYearDayPerilIdEventIdPtr, key);
                                currentYearDayPerilIdEventIdPtr += Vector256<long>.Count;
                            }
                            else if (Avx.IsSupported)
                            {
                                var key = ((Avx.ConvertToVector128Int64(tempCurrentDay)) << 49) | ((Avx.ConvertToVector128Int64(tempCurrentYear)) << 33) | ((Avx.ConvertToVector128Int64(tempCurrentPerilId)) << 32) | Avx.ConvertToVector128Int64(tempCurrentEventId);
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
                tempCurrentEventId = tempEventIds;
                while (tempCurrentYear < tempLastYear)
                {
                    //the key is identical using long instead of ulong for day in [1, 365], year in [0, 10000], perilId in [0, 255] and EventId in [1, Int32.MaxValue]. 
                    *currentYearDayPerilIdEventIdPtr++ = (((long)*tempCurrentDay++) << 49) | (((long)*tempCurrentYear++) << 33) | (((long)*tempCurrentPerilId++) << 32) | *tempCurrentEventId++;
                }
            }
        }

        public RevoLayerDayYeltVectorised2(in string filePath)
        {
            using (BinaryReader reader = new BinaryReader(new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
            {
                Span<byte> headerBuffer = new byte[RevoYeltBinaryWriter.CURRENT_BINARY_WRITER_HEADER_LENGTH];
                reader.Read(headerBuffer);

                fixed (byte* headerPtr = headerBuffer)
                {
                    LossAnalysisId = *(int*)(headerPtr + RevoYeltBinaryWriter.LOSSANALYSISID_INDEX);
                    LayerId = *(int*)(headerPtr + RevoYeltBinaryWriter.LAYERID_INDEX);
                    RowVersion = *(long*)(headerPtr + RevoYeltBinaryWriter.ROWVERSION_INDEX);
                    TotalEntryCount = *((int*)(headerPtr + RevoYeltBinaryWriter.TOTALENTRYCOUNT_INDEX));
                    HasRP = *(bool*)(headerPtr + RevoYeltBinaryWriter.HASRP_INDEX);
                    HasRB = *(bool*)(headerPtr + RevoYeltBinaryWriter.HASRB_INDEX);
                }

                _lastBufferIndex = TotalEntryCount / BUFFER_ITEM_COUNT;
                BufferCount = _lastBufferIndex + 1;
                _lastBufferItemCount = (TotalEntryCount % BUFFER_ITEM_COUNT);
                _lastBufferSizeShort = _lastBufferItemCount << 1;
                _lastBufferSizeInt = _lastBufferItemCount << 2;
                _lastBufferSizeDouble = _lastBufferItemCount << 3;
                _lastBufferSizeLong = _lastBufferSizeDouble;


                nuint ptrSize = (nuint)(Unsafe.SizeOf<IntPtr>() * BufferCount);
                _dayYearPerilIdEventIdKeys = (long**)NativeMemory.AlignedAlloc(ptrSize, (nuint)Unsafe.SizeOf<IntPtr>());
                _days = (short**)NativeMemory.AlignedAlloc(ptrSize, (nuint)Unsafe.SizeOf<IntPtr>());
                _lossPcts = (double**)NativeMemory.AlignedAlloc(ptrSize, (nuint)Unsafe.SizeOf<IntPtr>());
                _RPs = (double**)NativeMemory.AlignedAlloc(ptrSize, (nuint)Unsafe.SizeOf<IntPtr>());
                _RBs = (double**)NativeMemory.AlignedAlloc(ptrSize, (nuint)Unsafe.SizeOf<IntPtr>());

                Span<long> currentYearDayPerilIdEventIdSpan = Span<long>.Empty;
                int currentBufferIndex = 0;

                while(currentBufferIndex < _lastBufferIndex)
                {
                    _dayYearPerilIdEventIdKeys[currentBufferIndex] = (long*)NativeMemory.AlignedAlloc(BUFFER_SIZE_LONG, sizeof(long));
                    currentYearDayPerilIdEventIdSpan = new Span<long>(_dayYearPerilIdEventIdKeys[currentBufferIndex], BUFFER_ITEM_COUNT);
                    reader.Read(MemoryMarshal.Cast<long, byte>(currentYearDayPerilIdEventIdSpan));
                    currentBufferIndex++;
                }

                _dayYearPerilIdEventIdKeys[currentBufferIndex] = (long*)NativeMemory.AlignedAlloc((nuint)_lastBufferSizeLong, sizeof(long));
                currentYearDayPerilIdEventIdSpan = new Span<long>(_dayYearPerilIdEventIdKeys[currentBufferIndex], _lastBufferSizeLong >> 3);
                reader.Read(MemoryMarshal.Cast<long, byte>(currentYearDayPerilIdEventIdSpan));

                Span<short> currentDayBufferSpan = Span<short>.Empty;
                currentBufferIndex = 0;

                while (currentBufferIndex < _lastBufferIndex)
                {
                    _days[currentBufferIndex] = (short*)NativeMemory.AlignedAlloc(BUFFER_SIZE_SHORT, sizeof(short));
                    currentDayBufferSpan = new Span<short>(_days[currentBufferIndex], BUFFER_ITEM_COUNT);
                    reader.Read(MemoryMarshal.Cast<short, byte>(currentDayBufferSpan));
                    currentBufferIndex++;
                }

                _days[currentBufferIndex] = (short*)NativeMemory.AlignedAlloc((nuint)_lastBufferSizeLong, sizeof(short));
                currentDayBufferSpan = new Span<short>(_days[currentBufferIndex], _lastBufferSizeLong >> 1);
                reader.Read(MemoryMarshal.Cast<short, byte>(currentDayBufferSpan));
                
                Span<double> currentLossPctBufferSpan = Span<double>.Empty;
                currentBufferIndex = 0;

                while (currentBufferIndex < _lastBufferIndex)
                {
                    _lossPcts[currentBufferIndex] = (double*)NativeMemory.AlignedAlloc(BUFFER_SIZE_DOUBLE, sizeof(double));
                    currentLossPctBufferSpan = new Span<double>(_lossPcts[currentBufferIndex], BUFFER_ITEM_COUNT);
                    reader.Read(MemoryMarshal.Cast<double, byte>(currentLossPctBufferSpan));
                    currentBufferIndex++;
                }

                _lossPcts[currentBufferIndex] = (double*)NativeMemory.AlignedAlloc((nuint)_lastBufferSizeDouble, sizeof(double));
                currentLossPctBufferSpan = new Span<double>(_lossPcts[currentBufferIndex], _lastBufferSizeLong >> 3);
                reader.Read(MemoryMarshal.Cast<double, byte>(currentLossPctBufferSpan));

                if(HasRP)
                {
                    Span<double> currentRPBufferSpan = Span<double>.Empty;
                    currentBufferIndex = 0;

                    while (currentBufferIndex < _lastBufferIndex)
                    {
                        _RPs[currentBufferIndex] = (double*)NativeMemory.AlignedAlloc(BUFFER_SIZE_DOUBLE, sizeof(double));
                        currentRPBufferSpan = new Span<double>(_RPs[currentBufferIndex], BUFFER_ITEM_COUNT);
                        reader.Read(MemoryMarshal.Cast<double, byte>(currentRPBufferSpan));
                        currentBufferIndex++;
                    }

                    _RPs[currentBufferIndex] = (double*)NativeMemory.AlignedAlloc((nuint)_lastBufferSizeDouble, sizeof(double));
                    currentRPBufferSpan = new Span<double>(_RPs[currentBufferIndex], _lastBufferSizeLong >> 3);
                    reader.Read(MemoryMarshal.Cast<double, byte>(currentRPBufferSpan));
                }

                if(HasRB)
                {
                    Span<double> currentRBBufferSpan = Span<double>.Empty;
                    currentBufferIndex = 0;

                    while (currentBufferIndex < _lastBufferIndex)
                    {
                        _RBs[currentBufferIndex] = (double*)NativeMemory.AlignedAlloc(BUFFER_SIZE_DOUBLE, sizeof(double));
                        currentRBBufferSpan = new Span<double>(_RBs[currentBufferIndex], BUFFER_ITEM_COUNT);
                        reader.Read(MemoryMarshal.Cast<double, byte>(currentRBBufferSpan));
                        currentBufferIndex++;
                    }

                    _RBs[currentBufferIndex] = (double*)NativeMemory.AlignedAlloc((nuint)_lastBufferSizeDouble, sizeof(double));
                    currentRBBufferSpan = new Span<double>(_RBs[currentBufferIndex], _lastBufferSizeLong >> 3);
                    reader.Read(MemoryMarshal.Cast<double, byte>(currentRBBufferSpan));
                }
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
        public ReadOnlySpan<long> YearDayEventIdPerilIdKeys(in uint i) => new ReadOnlySpan<long>(_dayYearPerilIdEventIdKeys[i], i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

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
            if (!_disposed)
            {
                if (disposing)
                {
                }

                for (int i = 0; i < BufferCount; i++)
                {
                    NativeMemory.AlignedFree(_dayYearPerilIdEventIdKeys[i]);
                    NativeMemory.AlignedFree(_days[i]);
                    NativeMemory.AlignedFree(_lossPcts[i]);
                    NativeMemory.AlignedFree(_RPs[i]);
                    NativeMemory.AlignedFree(_RBs[i]);
                    _dayYearPerilIdEventIdKeys[i] = null;
                    _days[i] = null;
                    _lossPcts[i] = null;
                    _RPs[i] = null;
                    _RBs[i] = null;
                }

                NativeMemory.AlignedFree(_dayYearPerilIdEventIdKeys);
                NativeMemory.AlignedFree(_days);
                NativeMemory.AlignedFree(_lossPcts);
                NativeMemory.AlignedFree(_RPs);
                NativeMemory.AlignedFree(_RBs);
                _dayYearPerilIdEventIdKeys = null;
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
