
using Arch.ILS.EconomicModel.Binary;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Arch.ILS.EconomicModel
{
    public unsafe class RevoLayerDayYelt : IYelt
    {
        public const int DAY_COUNT = 10000;
        public const int DAY_BUFFER_SIZE = DAY_COUNT + 1;
        public const int BUFFER_ITEM_COUNT = 1024;
        public const int BUFFER_SIZE_BYTE = BUFFER_ITEM_COUNT;
        public const int BUFFER_SIZE_SHORT = BUFFER_ITEM_COUNT << 1;
        //public const int BUFFER_SIZE_INT = BUFFER_ITEM_COUNT << 2;
        public const int BUFFER_SIZE_DOUBLE = BUFFER_ITEM_COUNT << 3;
        public const int BUFFER_SIZE_LONG = BUFFER_ITEM_COUNT << 3;

        private nint[] _dayYearPerilIdEventIdKeys;
        private nint[] _days;
        private nint[] _lossPcts;
        private nint[] _RPs;
        private nint[] _RBs;
        private int _lastYearBufferIndex;
        private int _lastBufferIndex;
        private int _lastYearBufferItemCount, _lastYearBufferSizeShort, _lastYearBufferSizeInt;
        private int _lastBufferItemCount, _lastBufferSizeShort, _lastBufferSizeInt, _lastBufferSizeDouble, _lastBufferSizeLong;

        public RevoLayerDayYelt(in int lossAnalysisId, in int layerId, in IEnumerable<RevoLayerYeltEntry> yelt)
        {
            LossAnalysisId = lossAnalysisId;
            LayerId = layerId;
            Span<nint> dayRefs = stackalloc nint[DAY_BUFFER_SIZE];
            var comparer = new RevoLayerEntryYearPerilIdEventIdComparer();
            int count = 0; 
            bool anyRP = false;
            bool anyRB = false;

            foreach (var entry in yelt)
            {
                if (dayRefs[entry.Day] == nint.Zero)
                {
                    var newEntry = new SortedSet<RevoLayerYeltEntry>(comparer);
                    GCHandle handle = GCHandle.Alloc(newEntry);
                    dayRefs[entry.Day] = GCHandle.ToIntPtr(GCHandle.Alloc(newEntry));
                    if (!((SortedSet<RevoLayerYeltEntry>)handle.Target).Add(entry))
                        throw new Exception("Expected unique Year, Day, Event Peril");
                    anyRP |= (entry.RP != 0);
                    anyRB |= (entry.RB != 0);
                }
                else
                {
                    GCHandle handle = GCHandle.FromIntPtr(dayRefs[entry.Day]);
                    if (!((SortedSet<RevoLayerYeltEntry>)handle.Target).Add(entry))
                        throw new Exception("Expected unique Year, Day, Event Peril");
                    anyRP |= (entry.RP != 0);
                    anyRB |= (entry.RB != 0);
                }

                count++;
            }

            HasRP = anyRP || anyRB;
            HasRB = anyRB;
            TotalEntryCount = count;
            dayRefs = dayRefs[1..];
            int remainder = (TotalEntryCount % BUFFER_ITEM_COUNT);
            _lastBufferIndex = TotalEntryCount / BUFFER_ITEM_COUNT - (remainder == 0 ? 1 : 0);
            BufferCount = _lastBufferIndex + 1;
            _lastBufferItemCount = remainder == 0 ? BUFFER_ITEM_COUNT : remainder;
            _lastBufferSizeShort = _lastBufferItemCount << 1;
            //_lastBufferSizeInt = _lastBufferItemCount << 2;
            _lastBufferSizeDouble = _lastBufferItemCount << 3;
            _lastBufferSizeLong = _lastBufferSizeDouble;
            int currentBuffer = -1;
            int currentInBufferIndex = 0;

            _dayYearPerilIdEventIdKeys = new nint[BufferCount];
            _days = new nint[BufferCount];
            _lossPcts = new nint[BufferCount];
            _RPs = HasRP ? new nint[BufferCount] : null;
            _RBs = HasRB ? new nint[BufferCount] : null;
            ref nint spanStart = ref MemoryMarshal.GetReference(dayRefs);
            ref nint spanEnd = ref Unsafe.Add(ref spanStart, dayRefs.Length);
            Span<long> currentYearDayPerilIdEventIdBufferSpan = Span<long>.Empty;
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
                                _dayYearPerilIdEventIdKeys[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeLong);
                                _days[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeShort);
                                _lossPcts[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeDouble);
                                if (HasRP)
                                    _RPs[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeDouble);
                                if (HasRB)
                                    _RBs[currentBuffer] = Marshal.AllocHGlobal(_lastBufferSizeDouble);

                                currentYearDayPerilIdEventIdBufferSpan = new Span<long>(_dayYearPerilIdEventIdKeys[currentBuffer].ToPointer(), _lastBufferItemCount);
                                currentDayBufferSpan = new Span<short>(_days[currentBuffer].ToPointer(), _lastBufferItemCount);
                                currentLossPctBufferSpan = new Span<double>(_lossPcts[currentBuffer].ToPointer(), _lastBufferItemCount);
                                currentRPBufferSpan = HasRP ? new Span<double>(_RPs[currentBuffer].ToPointer(), _lastBufferItemCount) : Span<double>.Empty;
                                currentRBBufferSpan = HasRB ? new Span<double>(_RBs[currentBuffer].ToPointer(), _lastBufferItemCount) : Span<double>.Empty;
                            }
                            else
                            {
                                _dayYearPerilIdEventIdKeys[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_LONG);
                                _days[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_SHORT);
                                _lossPcts[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_DOUBLE);
                                if (HasRP)
                                    _RPs[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_DOUBLE);
                                if (HasRB)
                                    _RBs[currentBuffer] = Marshal.AllocHGlobal(BUFFER_SIZE_DOUBLE);

                                currentYearDayPerilIdEventIdBufferSpan = new Span<long>(_dayYearPerilIdEventIdKeys[currentBuffer].ToPointer(), BUFFER_ITEM_COUNT);
                                currentDayBufferSpan = new Span<short>(_days[currentBuffer].ToPointer(), BUFFER_ITEM_COUNT);
                                currentLossPctBufferSpan = new Span<double>(_lossPcts[currentBuffer].ToPointer(), BUFFER_ITEM_COUNT);
                                currentRPBufferSpan = HasRP ? new Span<double>(_RPs[currentBuffer].ToPointer(), BUFFER_ITEM_COUNT) : Span<double>.Empty;
                                currentRBBufferSpan = HasRB ? new Span<double>(_RBs[currentBuffer].ToPointer(), BUFFER_ITEM_COUNT) : Span<double>.Empty;
                            }

                            currentInBufferIndex = 0;
                        }

                        currentDayBufferSpan[currentInBufferIndex] = *entry.GetDay();
                        currentYearDayPerilIdEventIdBufferSpan[currentInBufferIndex] = (((long)(*entry.GetDay())) << 54) | (((long)(*entry.GetYear())) << 39) | (((long)(*entry.GetPerilId())) << 33) | (*entry.GetEventId());
                        currentLossPctBufferSpan[currentInBufferIndex] = *entry.GetLossPct();
                        if (HasRP)
                            currentRPBufferSpan[currentInBufferIndex] = *entry.GetRP();
                        if (HasRB)
                            currentRBBufferSpan[currentInBufferIndex] = *entry.GetRB();
                        currentInBufferIndex++;
                    }
                }

                spanStart = ref Unsafe.Add(ref spanStart, 1);
            }
        }

        public RevoLayerDayYelt(in string filePath, bool parallelise = false)
        {
            if (parallelise)
                ParallelRead(filePath);
            else
                Read(in filePath);
        }

        public int LossAnalysisId { get; private set; }
        public int LayerId { get; private set; }
        public long RowVersion { get; set; }
        public int BufferCount { get; private set; }
        public int TotalEntryCount { get; private set; }
        public bool HasRP { get; private set; }
        public bool HasRB { get; private set; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<long> YearDayEventIdPerilIdKeys(in uint i) => new ReadOnlySpan<long>(_dayYearPerilIdEventIdKeys[i].ToPointer(), i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<short> Days(in uint i) => new ReadOnlySpan<short>(_days[i].ToPointer(), i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<double> LossPcts(in uint i) => new ReadOnlySpan<double>(_lossPcts[i].ToPointer(), i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<double> RPs(in uint i) => new ReadOnlySpan<double>(_RPs[i].ToPointer(), i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<double> RBs(in uint i) => new ReadOnlySpan<double>(_RBs[i].ToPointer(), i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        private void Read(in string filePath)
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

                int remainder = (TotalEntryCount % BUFFER_ITEM_COUNT);
                _lastBufferIndex = TotalEntryCount / BUFFER_ITEM_COUNT - (remainder == 0 ? 1 : 0);
                BufferCount = _lastBufferIndex + 1;
                _lastBufferItemCount = remainder == 0 ? BUFFER_ITEM_COUNT : remainder;
                _lastBufferSizeShort = _lastBufferItemCount << 1;
                //_lastBufferSizeInt = _lastBufferItemCount << 2;
                _lastBufferSizeDouble = _lastBufferItemCount << 3;
                _lastBufferSizeLong = _lastBufferSizeDouble;

                _dayYearPerilIdEventIdKeys = new nint[BufferCount];
                _days = new nint[BufferCount];
                _lossPcts = new nint[BufferCount];
                _RPs = HasRP ? new nint[BufferCount] : null;
                _RBs = HasRB ? new nint[BufferCount] : null;

                Span<long> currentYearDayPerilIdEventIdSpan = Span<long>.Empty;
                int currentBufferIndex = 0;

                while (currentBufferIndex < _lastBufferIndex)
                {
                    _dayYearPerilIdEventIdKeys[currentBufferIndex] = Marshal.AllocHGlobal(BUFFER_SIZE_LONG); ;
                    currentYearDayPerilIdEventIdSpan = new Span<long>(_dayYearPerilIdEventIdKeys[currentBufferIndex].ToPointer(), BUFFER_ITEM_COUNT);
                    reader.Read(MemoryMarshal.Cast<long, byte>(currentYearDayPerilIdEventIdSpan));
                    currentBufferIndex++;
                }

                _dayYearPerilIdEventIdKeys[currentBufferIndex] = Marshal.AllocHGlobal(_lastBufferSizeLong);
                currentYearDayPerilIdEventIdSpan = new Span<long>(_dayYearPerilIdEventIdKeys[currentBufferIndex].ToPointer(), _lastBufferItemCount);
                reader.Read(MemoryMarshal.Cast<long, byte>(currentYearDayPerilIdEventIdSpan));

                Span<short> currentDayBufferSpan = Span<short>.Empty;
                currentBufferIndex = 0;

                while (currentBufferIndex < _lastBufferIndex)
                {
                    _days[currentBufferIndex] = Marshal.AllocHGlobal(BUFFER_SIZE_SHORT);
                    currentDayBufferSpan = new Span<short>(_days[currentBufferIndex].ToPointer(), BUFFER_ITEM_COUNT);
                    reader.Read(MemoryMarshal.Cast<short, byte>(currentDayBufferSpan));
                    currentBufferIndex++;
                }

                _days[currentBufferIndex] = Marshal.AllocHGlobal(_lastBufferSizeShort);
                currentDayBufferSpan = new Span<short>(_days[currentBufferIndex].ToPointer(), _lastBufferItemCount);
                reader.Read(MemoryMarshal.Cast<short, byte>(currentDayBufferSpan));

                Span<double> currentLossPctBufferSpan = Span<double>.Empty;
                currentBufferIndex = 0;

                while (currentBufferIndex < _lastBufferIndex)
                {
                    _lossPcts[currentBufferIndex] = Marshal.AllocHGlobal(BUFFER_SIZE_DOUBLE);
                    currentLossPctBufferSpan = new Span<double>(_lossPcts[currentBufferIndex].ToPointer(), BUFFER_ITEM_COUNT);
                    reader.Read(MemoryMarshal.Cast<double, byte>(currentLossPctBufferSpan));
                    currentBufferIndex++;
                }

                _lossPcts[currentBufferIndex] = Marshal.AllocHGlobal(_lastBufferSizeDouble);
                currentLossPctBufferSpan = new Span<double>(_lossPcts[currentBufferIndex].ToPointer(), _lastBufferItemCount);
                reader.Read(MemoryMarshal.Cast<double, byte>(currentLossPctBufferSpan));

                if (HasRP)
                {
                    Span<double> currentRPBufferSpan = Span<double>.Empty;
                    currentBufferIndex = 0;

                    while (currentBufferIndex < _lastBufferIndex)
                    {
                        _RPs[currentBufferIndex] = Marshal.AllocHGlobal(BUFFER_SIZE_DOUBLE);
                        currentRPBufferSpan = new Span<double>(_RPs[currentBufferIndex].ToPointer(), BUFFER_ITEM_COUNT);
                        reader.Read(MemoryMarshal.Cast<double, byte>(currentRPBufferSpan));
                        currentBufferIndex++;
                    }

                    _RPs[currentBufferIndex] = Marshal.AllocHGlobal(_lastBufferSizeDouble);
                    currentRPBufferSpan = new Span<double>(_RPs[currentBufferIndex].ToPointer(), _lastBufferItemCount);
                    reader.Read(MemoryMarshal.Cast<double, byte>(currentRPBufferSpan));
                }

                if (HasRB)
                {
                    Span<double> currentRBBufferSpan = Span<double>.Empty;
                    currentBufferIndex = 0;

                    while (currentBufferIndex < _lastBufferIndex)
                    {
                        _RBs[currentBufferIndex] = Marshal.AllocHGlobal(BUFFER_SIZE_DOUBLE);
                        currentRBBufferSpan = new Span<double>(_RBs[currentBufferIndex].ToPointer(), BUFFER_ITEM_COUNT);
                        reader.Read(MemoryMarshal.Cast<double, byte>(currentRBBufferSpan));
                        currentBufferIndex++;
                    }

                    _RBs[currentBufferIndex] = Marshal.AllocHGlobal(_lastBufferSizeDouble);
                    currentRBBufferSpan = new Span<double>(_RBs[currentBufferIndex].ToPointer(), _lastBufferItemCount);
                    reader.Read(MemoryMarshal.Cast<double, byte>(currentRBBufferSpan));
                }
            }
        }

        private void ParallelRead(string filePath)
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
            }

            int remainder = (TotalEntryCount % BUFFER_ITEM_COUNT);
            _lastBufferIndex = TotalEntryCount / BUFFER_ITEM_COUNT - (remainder == 0 ? 1 : 0);
            BufferCount = _lastBufferIndex + 1;
            _lastBufferItemCount = remainder == 0 ? BUFFER_ITEM_COUNT : remainder;
            _lastBufferSizeShort = _lastBufferItemCount << 1;
            //_lastBufferSizeInt = _lastBufferItemCount << 2;
            _lastBufferSizeDouble = _lastBufferItemCount << 3;
            _lastBufferSizeLong = _lastBufferSizeDouble;


            _dayYearPerilIdEventIdKeys = new nint[BufferCount];
            _days = new nint[BufferCount];
            _lossPcts = new nint[BufferCount];
            _RPs = HasRP ? new nint[BufferCount] : null;
            _RBs = HasRB ? new nint[BufferCount] : null;
            
            Task dayYearPerilIdEventIdTask = Task.Factory.StartNew(() =>
            {
                int currentPosition = RevoYeltBinaryWriter.CURRENT_BINARY_WRITER_HEADER_LENGTH;
                using (BinaryReader reader = new BinaryReader(new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    reader.BaseStream.Position = currentPosition;
                    Span<long> currentYearDayPerilIdEventIdSpan = Span<long>.Empty;
                    int currentBufferIndex = 0;

                    while (currentBufferIndex < _lastBufferIndex)
                    {
                        _dayYearPerilIdEventIdKeys[currentBufferIndex] = Marshal.AllocHGlobal(BUFFER_SIZE_LONG);
                        currentYearDayPerilIdEventIdSpan = new Span<long>(_dayYearPerilIdEventIdKeys[currentBufferIndex].ToPointer(), BUFFER_ITEM_COUNT);
                        reader.Read(MemoryMarshal.Cast<long, byte>(currentYearDayPerilIdEventIdSpan));
                        currentBufferIndex++;
                    }

                    _dayYearPerilIdEventIdKeys[currentBufferIndex] = Marshal.AllocHGlobal(_lastBufferSizeLong);
                    currentYearDayPerilIdEventIdSpan = new Span<long>(_dayYearPerilIdEventIdKeys[currentBufferIndex].ToPointer(), _lastBufferItemCount);
                    reader.Read(MemoryMarshal.Cast<long, byte>(currentYearDayPerilIdEventIdSpan));
                }
            });

            Task dayTask = Task.Factory.StartNew(() =>
            {
                int currentPosition = RevoYeltBinaryWriter.CURRENT_BINARY_WRITER_HEADER_LENGTH + (TotalEntryCount << 3);
                using (BinaryReader reader = new BinaryReader(new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    reader.BaseStream.Position = currentPosition;
                    Span<short> currentDayBufferSpan = Span<short>.Empty;
                    int currentBufferIndex = 0;

                    while (currentBufferIndex < _lastBufferIndex)
                    {
                        _days[currentBufferIndex] = Marshal.AllocHGlobal(BUFFER_SIZE_SHORT);
                        currentDayBufferSpan = new Span<short>(_days[currentBufferIndex].ToPointer(), BUFFER_ITEM_COUNT);
                        reader.Read(MemoryMarshal.Cast<short, byte>(currentDayBufferSpan));
                        currentBufferIndex++;
                    }

                    _days[currentBufferIndex] = Marshal.AllocHGlobal(_lastBufferSizeShort);
                    currentDayBufferSpan = new Span<short>(_days[currentBufferIndex].ToPointer(), _lastBufferItemCount);
                    reader.Read(MemoryMarshal.Cast<short, byte>(currentDayBufferSpan));
                }
            });

            Task lossPctTask = Task.Factory.StartNew(() =>
            {
                int currentPosition = RevoYeltBinaryWriter.CURRENT_BINARY_WRITER_HEADER_LENGTH + (TotalEntryCount << 3) + (TotalEntryCount << 1);
                using (BinaryReader reader = new BinaryReader(new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    reader.BaseStream.Position = currentPosition;
                    Span<double> currentLossPctBufferSpan = Span<double>.Empty;
                    int currentBufferIndex = 0;

                    while (currentBufferIndex < _lastBufferIndex)
                    {
                        _lossPcts[currentBufferIndex] = Marshal.AllocHGlobal(BUFFER_SIZE_DOUBLE);
                        currentLossPctBufferSpan = new Span<double>(_lossPcts[currentBufferIndex].ToPointer(), BUFFER_ITEM_COUNT);
                        reader.Read(MemoryMarshal.Cast<double, byte>(currentLossPctBufferSpan));
                        currentBufferIndex++;
                    }

                    _lossPcts[currentBufferIndex] = Marshal.AllocHGlobal(_lastBufferSizeDouble);
                    currentLossPctBufferSpan = new Span<double>(_lossPcts[currentBufferIndex].ToPointer(), _lastBufferItemCount);
                    reader.Read(MemoryMarshal.Cast<double, byte>(currentLossPctBufferSpan));
                }
            });

            Task RPTask = Task.Factory.StartNew(() =>
            {
                if (HasRP)
                {
                    int currentPosition = RevoYeltBinaryWriter.CURRENT_BINARY_WRITER_HEADER_LENGTH + (TotalEntryCount << 4) + (TotalEntryCount << 1);
                    using (BinaryReader reader = new BinaryReader(new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                    {
                        reader.BaseStream.Position = currentPosition;
                        Span<double> currentRPBufferSpan = Span<double>.Empty;
                        int currentBufferIndex = 0;

                        while (currentBufferIndex < _lastBufferIndex)
                        {
                            _RPs[currentBufferIndex] = Marshal.AllocHGlobal(BUFFER_SIZE_DOUBLE);
                            currentRPBufferSpan = new Span<double>(_RPs[currentBufferIndex].ToPointer(), BUFFER_ITEM_COUNT);
                            reader.Read(MemoryMarshal.Cast<double, byte>(currentRPBufferSpan));
                            currentBufferIndex++;
                        }

                        _RPs[currentBufferIndex] = Marshal.AllocHGlobal(_lastBufferSizeDouble);
                        currentRPBufferSpan = new Span<double>(_RPs[currentBufferIndex].ToPointer(), _lastBufferItemCount);
                        reader.Read(MemoryMarshal.Cast<double, byte>(currentRPBufferSpan));
                    }
                }
            });

            Task RBTask = Task.Factory.StartNew(() =>
            {
                if (HasRB)
                {
                    int currentPosition = RevoYeltBinaryWriter.CURRENT_BINARY_WRITER_HEADER_LENGTH + (TotalEntryCount << 5) + (TotalEntryCount << 1);
                    using (BinaryReader reader = new BinaryReader(new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                    {
                        reader.BaseStream.Position = currentPosition;
                        Span<double> currentRBBufferSpan = Span<double>.Empty;
                        int currentBufferIndex = 0;

                        while (currentBufferIndex < _lastBufferIndex)
                        {
                            _RBs[currentBufferIndex] = Marshal.AllocHGlobal(BUFFER_SIZE_DOUBLE);
                            currentRBBufferSpan = new Span<double>(_RBs[currentBufferIndex].ToPointer(), BUFFER_ITEM_COUNT);
                            reader.Read(MemoryMarshal.Cast<double, byte>(currentRBBufferSpan));
                            currentBufferIndex++;
                        }

                        _RBs[currentBufferIndex] = Marshal.AllocHGlobal(_lastBufferSizeDouble);
                        currentRBBufferSpan = new Span<double>(_RBs[currentBufferIndex].ToPointer(), _lastBufferItemCount);
                        reader.Read(MemoryMarshal.Cast<double, byte>(currentRBBufferSpan));
                    }
                }
            });

            Task.WaitAll(dayYearPerilIdEventIdTask, dayTask, lossPctTask, RPTask, RBTask);
        }

        public void Dispose(bool disposing)
        {
            for (int i = 0; i < _dayYearPerilIdEventIdKeys.Length; i++)
            {
                Marshal.FreeHGlobal(_dayYearPerilIdEventIdKeys[i]);
                Marshal.FreeHGlobal(_days[i]);
                Marshal.FreeHGlobal(_lossPcts[i]);
                Marshal.FreeHGlobal(_RPs[i]);
                Marshal.FreeHGlobal(_RBs[i]);
                _dayYearPerilIdEventIdKeys[i] = nint.Zero;
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
