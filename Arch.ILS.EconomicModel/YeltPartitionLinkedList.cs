
using System.Runtime.CompilerServices;

namespace Arch.ILS.EconomicModel
{
    public unsafe class YeltPartitionLinkedList
    {
        public YeltPartitionLinkedList(ref YeltDayPartition dayYearEventIdKeySpan)
        {
            fixed (short* startDayPtr = dayYearEventIdKeySpan.PartitionDays)
            {
                CurrentStartDay = startDayPtr;
                CurrentEndDay = CurrentStartDay + dayYearEventIdKeySpan.PartitionDays.Length - 1;
            }

            fixed (long* keyPtr = dayYearEventIdKeySpan.PartitionYearDayEventIdKeys)
            {
                CurrentStartKey = keyPtr;
                CurrentEndKey = CurrentStartKey + dayYearEventIdKeySpan.PartitionYearDayEventIdKeys.Length - 1;
                CurrentLength = dayYearEventIdKeySpan.PartitionYearDayEventIdKeys.Length;
            }

            fixed (double* lossPctPtr = dayYearEventIdKeySpan.PartitionLossPcts)
            {
                CurrentStartLossPct = lossPctPtr;
                CurrentEndLossPct = CurrentStartLossPct + dayYearEventIdKeySpan.PartitionLossPcts.Length - 1;
            }

            fixed (double* rpPtr = dayYearEventIdKeySpan.PartitionRPs)
            {
                CurrentStartRP = rpPtr;
                CurrentEndRP = CurrentStartRP + dayYearEventIdKeySpan.PartitionRPs.Length - 1;
            }

            fixed (double* rbPtr = dayYearEventIdKeySpan.PartitionRBs)
            {
                CurrentStartRB = rbPtr;
                CurrentEndRB = CurrentStartRB + dayYearEventIdKeySpan.PartitionRBs.Length - 1;
            }
        }

        public short* CurrentStartDay;
        public short* CurrentEndDay;
        public long* CurrentStartKey;
        public long* CurrentEndKey;
        public double* CurrentStartLossPct;
        public double* CurrentEndLossPct;
        public double* CurrentStartRP;
        public double* CurrentEndRP;
        public double* CurrentStartRB;
        public double* CurrentEndRB;
        public YeltPartitionLinkedList NextNode;
        public int CurrentLength { get; }
        public int TotalLength => CurrentLength + (NextNode?.TotalLength ?? 0);

        public void AddNext(ref YeltPartitionLinkedList nextNode)
        {
            NextNode = nextNode;
        }
    }
}
