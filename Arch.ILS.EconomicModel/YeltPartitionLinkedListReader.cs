
namespace Arch.ILS.EconomicModel
{
    public unsafe class YeltPartitionLinkedListReader
    {
        private static int Zero = 0;
        public unsafe YeltPartitionLinkedListReader(ref YeltPartitionLinkedList yeltPartitionLinkedList)
        {
            Head = yeltPartitionLinkedList;
            CurrentPartition = Head;
            CurrentPartitionCurrentItem = CurrentPartition.CurrentStartKey;
            CurrentPartitionLastItem = CurrentPartition.CurrentEndKey;
            MoveNext = yeltPartitionLinkedList.TotalLength > 0;
        }

        public YeltPartitionLinkedList Head;
        public YeltPartitionLinkedList CurrentPartition;
        public long* CurrentPartitionCurrentItem;
        public long* CurrentPartitionLastItem;
        public bool MoveNext;

        public unsafe void SetNext()
        {
            if(CurrentPartitionCurrentItem < CurrentPartitionLastItem)
            {
                CurrentPartitionCurrentItem++;
            }
            else if(CurrentPartition.NextNode != null)
            {
                CurrentPartition = CurrentPartition.NextNode;
                CurrentPartitionCurrentItem = CurrentPartition.CurrentStartKey;
                CurrentPartitionLastItem = CurrentPartition.CurrentEndKey;
            }
            else
            {
                MoveNext = false;
            }
        }

        public unsafe static YeltPartitionLinkedListReader Initialise(YeltPartitioner yeltPartitioner)
        {
            YeltPartitionLinkedList headPtr = null;
            YeltPartitionLinkedList currentPtr = null;
            while (yeltPartitioner.MoveNext)
            {
                if (yeltPartitioner.TryGetCurrentPartition(out var partition))
                {
                    if (headPtr == null)
                    {
                        headPtr = new(ref partition);
                        currentPtr = headPtr;
                    }
                    else
                    {
                        YeltPartitionLinkedList nextNode = new(ref partition);
                        currentPtr.AddNext(ref nextNode);
                        currentPtr = nextNode;
                    }
                }
            }

            return new YeltPartitionLinkedListReader(ref headPtr);
        }
    }
}
