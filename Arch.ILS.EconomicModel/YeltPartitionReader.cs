
namespace Arch.ILS.EconomicModel
{
    public unsafe class YeltPartitionReader
    {
        private static int Zero = 0;
        public unsafe YeltPartitionReader(in IYelt yelt, ref YeltPartition yeltPartitionLinkedList)
        {
            Yelt = yelt;
            Head = yeltPartitionLinkedList;
            CurrentPartition = Head;
            CurrentPartitionCurrentItem = CurrentPartition.CurrentStartKey;
            CurrentPartitionLastItem = CurrentPartition.CurrentEndKey;
            IsOpen = yeltPartitionLinkedList.TotalLength > 0;
        }

        public IYelt Yelt { get; }
        public YeltPartition Head;
        public YeltPartition CurrentPartition;
        public long* CurrentPartitionCurrentItem;
        public long* CurrentPartitionLastItem;
        public bool IsOpen;
        public int TotalLength => Head.TotalLength;

        public unsafe bool SetNext()
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
                IsOpen = false;
                CurrentPartition = null;
                CurrentPartitionCurrentItem = null;
                CurrentPartitionLastItem = null;
            }
            return IsOpen;
        }

        public void Reset()
        {
            CurrentPartition = Head;
            CurrentPartitionCurrentItem = CurrentPartition.CurrentStartKey;
            CurrentPartitionLastItem = CurrentPartition.CurrentEndKey;
            IsOpen = Head.TotalLength > 0;
        }

        public unsafe static YeltPartitionReader Initialise(YeltPartitioner yeltPartitioner)
        {
            YeltPartition headPtr = null;
            YeltPartition currentPtr = null;
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
                        YeltPartition nextNode = new(ref partition);
                        currentPtr.AddNext(nextNode);
                        currentPtr = nextNode;
                    }
                }
            }

            return new YeltPartitionReader(yeltPartitioner.Yelt, ref headPtr);
        }
    }
}
