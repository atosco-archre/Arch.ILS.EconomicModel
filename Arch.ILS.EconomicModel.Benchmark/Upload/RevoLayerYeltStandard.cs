﻿
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Arch.ILS.EconomicModel.Benchmark
{
    public unsafe class RevoLayerYeltStandard : IBenchmarkYelt, IDisposable
    {
        public const int YEAR_COUNT = 10_000;
        public const int YEAR_BUFFER_SIZE = YEAR_COUNT + 1;
        public const int BUFFER_ITEM_COUNT = 1024;

        private readonly short[][] _distinctYears;
        private readonly int[][] _yearRepeatCount;
        private readonly short[][] _days;
        private readonly int[][] _eventIds;
        private readonly double[][] _lossPcts;
        private readonly double[][] _RPs;
        private readonly double[][] _RBs;
        private readonly int _lastYearBufferIndex;
        private readonly int _lastBufferIndex;
        private int _lastYearBufferItemCount;
        private int _lastBufferItemCount;
        private bool _disposed;

        public RevoLayerYeltStandard(in int lossAnalysisId, in int layerId, in IEnumerable<RevoLayerYeltEntry> yelt)
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
            _lastBufferItemCount = (count % BUFFER_ITEM_COUNT);
            int currentYearBuffer = -1;
            int currentBuffer = -1;
            int currentYearInBufferIndex = 0;
            int currentInBufferIndex = 0;
            //int yearEndIndexExclusive = 0;

            _distinctYears = new short[YearBufferCount][];
            _yearRepeatCount = new int[YearBufferCount][];
            _days = new short[BufferCount][];
            _eventIds = new int[BufferCount][];
            _lossPcts = new double[BufferCount][];
            _RPs = new double[BufferCount][];
            _RBs = new double[BufferCount][];
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
                                _days[currentBuffer] = new short[_lastBufferItemCount];
                                _eventIds[currentBuffer] = new int[_lastBufferItemCount];
                                _lossPcts[currentBuffer] = new double[_lastBufferItemCount];
                                _RPs[currentBuffer] = new double[_lastBufferItemCount];
                                _RBs[currentBuffer] = new double[_lastBufferItemCount];
                            }
                            else
                            {
                                _days[currentBuffer] = ArrayPool<short>.Shared.Rent(BUFFER_ITEM_COUNT);
                                _eventIds[currentBuffer] = ArrayPool<int>.Shared.Rent(BUFFER_ITEM_COUNT);
                                _lossPcts[currentBuffer] = ArrayPool<double>.Shared.Rent(BUFFER_ITEM_COUNT);
                                _RPs[currentBuffer] = ArrayPool<double>.Shared.Rent(BUFFER_ITEM_COUNT);
                                _RBs[currentBuffer] = ArrayPool<double>.Shared.Rent(BUFFER_ITEM_COUNT);
                            }
                            currentDayBufferSpan = _days[currentBuffer];
                            currentEventIdBufferSpan = _eventIds[currentBuffer];
                            currentLossPctBufferSpan = _lossPcts[currentBuffer];
                            currentRPBufferSpan = _RPs[currentBuffer];
                            currentRBBufferSpan = _RBs[currentBuffer];
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
                            _distinctYears[currentYearBuffer] = new short[_lastYearBufferItemCount];
                            _yearRepeatCount[currentYearBuffer] = new int[_lastYearBufferItemCount];
                        }
                        else
                        {
                            _distinctYears[currentYearBuffer] = ArrayPool<short>.Shared.Rent(BUFFER_ITEM_COUNT);
                            _yearRepeatCount[currentYearBuffer] = ArrayPool<int>.Shared.Rent(BUFFER_ITEM_COUNT);
                        }
                        currentYearBufferSpan = _distinctYears[currentYearBuffer];
                        currentYearRepeatCountBufferSpan = _yearRepeatCount[currentYearBuffer];
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
        public ReadOnlySpan<short> DistinctYears(in uint i) =>  new ReadOnlySpan<short>(_distinctYears[i], 0, i == _lastYearBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<int> YearRepeatCount(in uint i) =>  new ReadOnlySpan<int>(_yearRepeatCount[i], 0, i == _lastYearBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<short> Days(in uint i) => new ReadOnlySpan<short>(_days[i], 0, i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<int> EventIds(in uint i) => new ReadOnlySpan<int>(_eventIds[i], 0, i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<double> LossPcts(in uint i) => new ReadOnlySpan<double>(_lossPcts[i], 0, i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<double> RPs(in uint i) => new ReadOnlySpan<double>(_RPs[i], 0, i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<double> RBs(in uint i) => new ReadOnlySpan<double>(_RBs[i], 0, i == _lastBufferIndex ? _lastBufferItemCount : BUFFER_ITEM_COUNT);

        public void Dispose(bool disposing)
        {
            if(!_disposed)
            {
                if (disposing)
                {
                    for (int i = 0; i < _distinctYears.Length; i++)
                    {
                        if (i != _distinctYears.Length - 1)
                        {
                            ArrayPool<short>.Shared.Return(_distinctYears[i]);
                            ArrayPool<int>.Shared.Return(_yearRepeatCount[i]);
                        }
                        _distinctYears[i] = Array.Empty<short>();
                        _yearRepeatCount[i] = Array.Empty<int>();
                    }

                    for (int i = 0; i < _eventIds.Length; i++)
                    {
                        if (i != _eventIds.Length - 1)
                        {
                            ArrayPool<int>.Shared.Return(_eventIds[i]);
                            ArrayPool<short>.Shared.Return(_days[i]);
                            ArrayPool<double>.Shared.Return(_lossPcts[i]);
                            ArrayPool<double>.Shared.Return(_RPs[i]);
                            ArrayPool<double>.Shared.Return(_RBs[i]);
                        }
                        _eventIds[i] = Array.Empty<int>();
                        _days[i] = Array.Empty<short>();
                        _lossPcts[i] = Array.Empty<double>();
                        _RPs[i] = Array.Empty<double>();
                        _RBs[i] = Array.Empty<double>();
                    }
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
