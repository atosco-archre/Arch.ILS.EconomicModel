
using System.Numerics;
using System.Runtime.Intrinsics;

namespace Arch.ILS.EconomicModel
{
    public class YeltPartitionMapper
    {
        public const int DefaultBufferSize = 1024;

        public YeltPartitionMapper(IEnumerable<YeltPartitionReader> yeltPartitionReaders, long[] sortedKeys, int bufferSize = DefaultBufferSize)
        { 
            YeltPartitionReaders = yeltPartitionReaders.Where(x => x.IsOpen).ToArray();
            TotalLength = YeltPartitionReaders.Sum(x => x.TotalLength);
            SortedKeys = sortedKeys;
            BufferSize = bufferSize;
        }

        public IList<YeltPartitionReader> YeltPartitionReaders { get; }
        public int TotalLength { get; }
        public long[] SortedKeys { get; }
        public int BufferSize { get; }

        public unsafe (int[] MappedIndices, int[] StartIndicesInMappedIndices) MapKeys()
        {
            int keysLength = SortedKeys.Length;
            int[] mappedIndices = new int[TotalLength];
            int[] startIndexInMappedIndices = new int[YeltPartitionReaders.Count];
            fixed (long* sortedKeys = SortedKeys)
            {
                long* currentKey = sortedKeys;
                long* lastKey = sortedKeys + keysLength - 1;
                var end = lastKey + 1;
                YeltPartitionIndexer[] indexers = new YeltPartitionIndexer[YeltPartitionReaders.Count];
                int indexerStartIndex = 0;
                fixed (int* keysPtr = mappedIndices)
                {
                    int* currentKeysPtr = keysPtr;
                    indexers[0] = new YeltPartitionIndexer(YeltPartitionReaders[0], currentKeysPtr);
                    startIndexInMappedIndices[0] = indexerStartIndex;
                    for (int i = 1; i < indexers.Length; i++)
                    {
                        indexers[i] = new YeltPartitionIndexer(YeltPartitionReaders[i], indexers[i - 1].CurrentPosition + YeltPartitionReaders[i - 1].TotalLength);
                        startIndexInMappedIndices[i] = (indexerStartIndex += YeltPartitionReaders[i - 1].TotalLength);
                    }                                            
                }

                int currentKeyIndex = 0;
                using (DynamicDirectory<Int64Span, YeltPartitionIndexer> keyIndexMapper = new(YeltPartitionReaders.Count))
                {
                    for (int r = 0; r < indexers.Length; r++)
                    {
                        YeltPartitionIndexer indexer = indexers[r];
                        if (indexer.YeltPartitionReader.IsOpen)
                        {
                            var newKey = new Int64Span(indexer.YeltPartitionReader.CurrentPartitionCurrentItem);
                            keyIndexMapper.Add(ref newKey, ref indexer);
                        }
                    }

                    while (currentKey != end)
                    {
                        Int64Span key = new Int64Span(currentKey);
                        var entry = keyIndexMapper.GetFirstRef(ref key);

                        while (entry != null)
                        {
                            YeltPartitionIndexer indexer = entry->value;
                            YeltPartitionReader reader = indexer.YeltPartitionReader;

                            if (*reader.CurrentPartitionCurrentItem == *currentKey)
                            {
                                indexer.Write(ref currentKeyIndex);
                                var tempEntry = keyIndexMapper.GetNextRef(ref key, entry);
                                if (reader.SetNext())
                                {
                                    var newKey = new Int64Span(reader.CurrentPartitionCurrentItem);
                                    keyIndexMapper.Move(ref newKey, entry);
                                }
                                entry = tempEntry;
                            }
                            else
                            {
                                var tempEntry = keyIndexMapper.GetNextRef(ref key, entry);
                                entry = tempEntry;
                            }
                        }

                        currentKey++;
                        currentKeyIndex++;
                    }
                }
            }

            return (mappedIndices, startIndexInMappedIndices);
        }

        public unsafe double[] Process(double cession, int maxDegreeOfParallelism = 2)
        {
            (int[] mappedIndices, int[] startIndicesInMappedIndices) = MapKeys(); 
            Reset();
            double[] eventLosses = new double[SortedKeys.Length];
            Span<double> s_EventLosses = new Span<double>(eventLosses);


#if DEBUG
            for (int k = 0; k < YeltPartitionReaders.Count; ++k)
#else
            Parallel.For(0, YeltPartitionReaders.Count, k =>
#endif
            {
                fixed (int* startIndicesInMappedIndicesPtr = startIndicesInMappedIndices)
                fixed (int* mappedIndicesPtr = mappedIndices)
                {
                    int startIndiexInMappedIndices = *(startIndicesInMappedIndicesPtr + k);
                    int* mappedIndicesCurrentIndexPtr = mappedIndicesPtr + startIndiexInMappedIndices;
                    YeltPartitionReader currentReader = YeltPartitionReaders[k];
                    YeltPartition yeltPartition = currentReader.Head;
                    while (yeltPartition != null)
                    {
                        int nonVectorStartIndex = 0;
                        double* currentPtr = yeltPartition.CurrentStartLossPct;
                        if (Vector512.IsHardwareAccelerated)
                        {
                            int inVectorCount = Vector512<double>.Count;
                            int outVectorCount = yeltPartition.CurrentLength / inVectorCount;
                            nonVectorStartIndex = outVectorCount * inVectorCount;
                            Vector512<double> v_Cession = Vector512.Create(cession);
                            for (int o = 0; o < outVectorCount; ++o)
                            {
                                Vector512<double> v_Cession_LossPcts = Vector512.Load<double>(currentPtr) * v_Cession;
                                currentPtr += inVectorCount;
                                for (int i = 0; i < inVectorCount; ++i)
                                    Add(ref eventLosses[*mappedIndicesCurrentIndexPtr++], v_Cession_LossPcts[i]);
                            }
                        }
                        else if (Vector256.IsHardwareAccelerated)
                        {
                            int inVectorCount = Vector256<double>.Count;
                            int outVectorCount = yeltPartition.CurrentLength / inVectorCount;
                            nonVectorStartIndex = outVectorCount * inVectorCount;
                            Vector256<double> v_Cession = Vector256.Create(cession);
                            for (int o = 0; o < outVectorCount; ++o)
                            {
                                Vector256<double> v_Cession_LossPcts = Vector256.Load<double>(currentPtr) * v_Cession;
                                currentPtr += inVectorCount;
                                for (int i = 0; i < inVectorCount; ++i)
                                    Add(ref eventLosses[*mappedIndicesCurrentIndexPtr++], v_Cession_LossPcts[i]);
                            }
                        }
                        else if (Vector.IsHardwareAccelerated)
                        {
                            int inVectorCount = Vector<double>.Count;
                            int outVectorCount = yeltPartition.CurrentLength / inVectorCount;
                            nonVectorStartIndex = outVectorCount * inVectorCount;
                            Vector<double> v_Cession = new Vector<double>(cession);
                            for (int o = 0; o < outVectorCount; ++o)
                            {
                                Vector<double> v_Cession_LossPcts = Vector.Load<double>(currentPtr) * v_Cession;
                                currentPtr += inVectorCount;
                                for(int i = 0; i < inVectorCount; ++i)
                                    Add(ref eventLosses[*mappedIndicesCurrentIndexPtr++], v_Cession_LossPcts[i]);
                            }
                        }

                        for (int i = nonVectorStartIndex; i < yeltPartition.CurrentLength; ++i)
                        {
                            Add(ref eventLosses[*mappedIndicesCurrentIndexPtr++], (*currentPtr++) * cession);
                        }

                        yeltPartition = yeltPartition.NextNode;
                    }
                }
            }
#if !DEBUG
            );
#endif


            return eventLosses;
        }

        public void Reset()
        {
            foreach (var reader in YeltPartitionReaders)
                reader.Reset();
        }

        private static unsafe double Add(ref double location1, double value)
        {
            double newCurrentValue = location1;
            while (true)
            {
                double currentValue = newCurrentValue;
                double newValue = currentValue + value;
                newCurrentValue = Interlocked.CompareExchange(ref location1, newValue, currentValue);
                if (newCurrentValue.Equals(currentValue))
                    return newValue;
            }
        }
    }
}
