
namespace Arch.ILS.EconomicModel.Benchmark
{
    public unsafe class YeltPartitionIndexer
    {
        private readonly int* _endPosition;

        public YeltPartitionIndexer(YeltPartitionReader yeltPartitionReader, int* currentPosition) 
        {
            YeltPartitionReader = yeltPartitionReader;
            CurrentPosition = currentPosition;
            _endPosition = currentPosition + yeltPartitionReader.TotalLength;
        }

        public YeltPartitionReader YeltPartitionReader{ get; }
        public int* CurrentPosition;
        public bool IsClosed => CurrentPosition == _endPosition;

        public void Write(ref int index)
        {
            *CurrentPosition++ = index;
        }

        public void Write(int* index)
        {
            *CurrentPosition++ = *index;
        }
    }
}
