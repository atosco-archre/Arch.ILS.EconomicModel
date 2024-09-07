
namespace Arch.ILS.EconomicModel
{
    public unsafe ref struct YeltPartitionLinkedList
    {
        public YeltPartitionLinkedList(ref YeltDayPartition dayYearEventIdKeySpan)
        {
            CurrentNode = dayYearEventIdKeySpan;
        }

        public YeltDayPartition CurrentNode;
        public YeltPartitionLinkedList* NextNode;
        public int TotalLength => CurrentNode.PartitionYearDayEventIdKeys.Length + (NextNode == null ? 0 : (*NextNode).TotalLength);

        public void AddNext(ref YeltPartitionLinkedList nextNode)
        {
            fixed(YeltPartitionLinkedList* nextPtr = &nextNode)
            {
                NextNode = nextPtr;
            }
        }
    }
}
