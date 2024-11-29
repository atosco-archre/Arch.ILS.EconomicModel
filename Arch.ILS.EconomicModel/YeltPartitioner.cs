
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Arch.ILS.EconomicModel
{    
    public class YeltPartitioner
    {
        private readonly short[] _startDays;
        private readonly short[] _endDays;        
        private uint _dayIndex = 0, _outBufferIndex = 0;
        private int _inBufferIndex = 0;

        public YeltPartitioner(in Range[] dayRanges, in IYelt yelt)
        {
            Yelt = yelt;
            short previousEndDay = -1;
            _startDays = new short[dayRanges.Length];
            _endDays = new short[dayRanges.Length];
            int i = 0;
            foreach (Range range in dayRanges.OrderBy(x => x.Start))
            {
                if (range.Start.Value < previousEndDay)
                    throw new Exception("Overlapping day ranges not allowed.");

                _startDays[i] = (short)range.Start.Value;
                _endDays[i++] = (short)range.End.Value;
            }
            MoveNext = true;
        }

        public IYelt Yelt { get; }
        public ref short CurrentStartDayInclusive => ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_startDays), _dayIndex);
        public ref short CurrentEndDayExclusive => ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_endDays), _dayIndex);
        public bool MoveNext { get; private set; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetCurrentPartition(out YeltDayPartition yeltDayPartition)
        {
            //Note that the end index is exclusive.
            ref short currentStartDay = ref CurrentStartDayInclusive;
            ref short currentEndDay = ref CurrentEndDayExclusive;

            while (_outBufferIndex < Yelt.BufferCount)
            {
                ReadOnlySpan<short> days = Yelt.Days(_outBufferIndex)[_inBufferIndex..];
                ReadOnlySpan<long> yearDayPerilIdEventIdKeys = Yelt.YearDayEventIdPerilIdKeys(_outBufferIndex)[_inBufferIndex..];
                ReadOnlySpan<double> lossPcts = Yelt.LossPcts(_outBufferIndex)[_inBufferIndex..];
                ReadOnlySpan<double> rps = Yelt.HasRP ? Yelt.RPs(_outBufferIndex)[_inBufferIndex..] : ReadOnlySpan<double>.Empty;
                ReadOnlySpan<double> rbs = Yelt.HasRB ? Yelt.RBs(_outBufferIndex)[_inBufferIndex..] : ReadOnlySpan<double>.Empty;
                int startIndex = currentStartDay <= days[0] ? 0 : days.BinarySearch<short>(currentStartDay);
                if (startIndex < 0)
                {
                    startIndex = ~startIndex;
                    if ((uint)startIndex == (uint)days.Length)
                    {
                        _outBufferIndex++;
                        _inBufferIndex = 0;
                        continue;
                    }                        
                }
                else if(startIndex > 0)
                {
                    int previousStartIndex = days[..startIndex].BinarySearch<short>((short)(currentStartDay - 1));
                    if (previousStartIndex < 0)
                    {
                        startIndex = ~previousStartIndex;
                    }
                    else
                    {
                        startIndex = ++previousStartIndex + days[previousStartIndex..(startIndex + 1)].IndexOf(currentStartDay);
                    }
                }

                days = days[startIndex..];
                int length = days.BinarySearch<short>(currentEndDay);

                if (length < 0)
                {
                    length = ~length;
                    if ((uint)length == (uint)days.Length)
                    {
                        _outBufferIndex++;
                        _inBufferIndex = 0;
                    }
                    else
                    {
                        _dayIndex++;
                        _inBufferIndex = startIndex + length;
                        if (_dayIndex >= (uint)_startDays.Length)
                            MoveNext = false;//prevent continuing to try get partition when all day partitions are processed.
                    }
                }
                else
                {
                    int previousEndIndex = days[..length].BinarySearch<short>((short)(currentEndDay - 1));
                    if (previousEndIndex < 0)
                    {
                        length = ~previousEndIndex;
                    }
                    else
                    {
                        length = ++previousEndIndex + days[previousEndIndex..length].IndexOf(currentEndDay);
                    }
                    _dayIndex++;
                    _inBufferIndex = startIndex + length;
                    if (_dayIndex >= (uint)_startDays.Length)
                        MoveNext = false;//prevent continuing to try get partition when all day partitions are processed.
                }

                days = days[..length];
                yeltDayPartition = new YeltDayPartition(days, yearDayPerilIdEventIdKeys.Slice(startIndex, length), lossPcts.Slice(startIndex, length), Yelt.HasRP ? rps.Slice(startIndex, length) : ReadOnlySpan<double>.Empty, Yelt.HasRB ? rbs.Slice(startIndex, length) : ReadOnlySpan<double>.Empty);
                return true;
            }

            MoveNext = false;
            yeltDayPartition = default;
            return false;
        }
    }
}
