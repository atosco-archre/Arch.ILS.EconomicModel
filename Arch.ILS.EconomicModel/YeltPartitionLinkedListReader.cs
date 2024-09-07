using Org.BouncyCastle.Asn1.Cmp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Arch.ILS.EconomicModel
{
    public ref struct YeltPartitionLinkedListReader
    {
        private static int Zero = 0;
        public unsafe YeltPartitionLinkedListReader(ref YeltPartitionLinkedList yeltPartitionLinkedList)
        {
            Head = yeltPartitionLinkedList;
            CurrentPartition = Head;
            CurrentPartitionCurrentItem = ref MemoryMarshal.GetReference(CurrentPartition.CurrentNode.PartitionYearDayEventIdKeys);
            CurrentPartitionLastItem = ref Unsafe.Add(ref CurrentPartitionCurrentItem, CurrentPartition.CurrentNode.PartitionYearDayEventIdKeys.Length - 1);
            MoveNext = true;
        }

        public YeltPartitionLinkedList Head;
        public YeltPartitionLinkedList CurrentPartition;
        public ref long CurrentPartitionCurrentItem;
        public ref long CurrentPartitionLastItem;
        public bool MoveNext;

        public unsafe void SetNext()
        {
            if(Unsafe.IsAddressLessThan(ref CurrentPartitionCurrentItem, ref CurrentPartitionLastItem))
            {
                CurrentPartitionCurrentItem = ref Unsafe.Add(ref CurrentPartitionCurrentItem, 1);
            }
            else if(CurrentPartition.NextNode != null)
            {
                CurrentPartition = *CurrentPartition.NextNode;
                CurrentPartitionCurrentItem = ref MemoryMarshal.GetReference(CurrentPartition.CurrentNode.PartitionYearDayEventIdKeys);
                CurrentPartitionLastItem = ref Unsafe.Add(ref CurrentPartitionCurrentItem, CurrentPartition.CurrentNode.PartitionYearDayEventIdKeys.Length - 1);
            }
            else
            {
                MoveNext = false;
            }
        }

        public unsafe static YeltPartitionLinkedListReader Initialise(YeltPartitioner yeltPartitioner)
        {
            YeltPartitionLinkedList* headPtr = null;
            YeltPartitionLinkedList* currentPtr = null;
            while (yeltPartitioner.MoveNext)
            {
                if (yeltPartitioner.TryGetCurrentPartition(out var partition))
                {
                    if (headPtr == null)
                    {
                        *headPtr = new(ref partition);
                        currentPtr = headPtr;
                    }
                    else
                    {
                        YeltPartitionLinkedList nextNode = new(ref partition);
                        (*currentPtr).AddNext(ref nextNode);
                        *currentPtr = nextNode;
                    }
                }
            }

            return new YeltPartitionLinkedListReader(ref *headPtr);
        }
    }
}
