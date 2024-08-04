
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Arch.ILS.EconomicModel
{    
    public class YeltPartitioner
    {
        private readonly short[] _startDays;
        private readonly short[] _endDays;
        private readonly IYelt _yelt;
        private uint _dayIndex = 0, _outBufferIndex = 0;
        private int _inBufferIndex = 0;

        public YeltPartitioner(in Range[] dayRanges, in IYelt yelt)
        {
            _yelt = yelt;
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

        public ref short CurrentStartDay => ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_endDays), _dayIndex);
        public ref short CurrentEndDay => ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_endDays), _dayIndex);
        public bool MoveNext { get; private set; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetCurrentPartition(out YeltDayPartition yeltDayPartition)
        {
            //Note that the end index is exclusive.
            ref short currentStartDay = ref CurrentStartDay;
            ref short currentEndDay = ref CurrentEndDay;

            while (_outBufferIndex < _yelt.BufferCount)
            {
                ReadOnlySpan<short> days = _yelt.Days(_outBufferIndex)[_inBufferIndex..];
                ReadOnlySpan<long> yearDayEventIdKeys = _yelt.YearDayEventIdKeys(_outBufferIndex)[_inBufferIndex..];
                ReadOnlySpan<double> lossPcts = _yelt.LossPcts(_outBufferIndex)[_inBufferIndex..];
                ReadOnlySpan<double> rps = _yelt.RPs(_outBufferIndex)[_inBufferIndex..];
                ReadOnlySpan<double> rbs = _yelt.RBs(_outBufferIndex)[_inBufferIndex..];
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

                days = days[startIndex..];
                int endIndex = days.BinarySearch<short>(currentEndDay);
                if (endIndex < 0)
                    endIndex = ~endIndex;

                if ((uint)endIndex == (uint)days.Length)
                {
                    _outBufferIndex++;
                    _inBufferIndex = 0;
                }
                else
                {
                    _dayIndex++;
                    _inBufferIndex = endIndex;
                    if (_dayIndex >= (uint)_startDays.Length)
                        MoveNext = false;//prevent continuing to try get partition when all day partitions are processed.
                }                    
                
                days = days[..endIndex];
                yeltDayPartition = new YeltDayPartition(days, yearDayEventIdKeys[startIndex..endIndex], lossPcts[startIndex..endIndex], rps[startIndex..endIndex], rbs[startIndex..endIndex]);
                return true;
            }

            MoveNext = false;
            yeltDayPartition = default;
            return false;
        }
    }
}
