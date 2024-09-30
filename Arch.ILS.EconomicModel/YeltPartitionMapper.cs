
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

        public unsafe int[] MapKeys()
        {
            int keysLength = SortedKeys.Length;
            int[] keys = new int[TotalLength];
            fixed (long* sortedKeys = SortedKeys)
            {
                long* currentKey = sortedKeys;
                long* lastKey = sortedKeys + keysLength - 1;
                var end = lastKey + 1;
                int currentKeyIndex = 0;
                YeltPartitionIndexer[] indexers = new YeltPartitionIndexer[YeltPartitionReaders.Count];
                fixed (int* keysPtr = keys)
                {
                    int* currentKeysPtr = keysPtr;
                    indexers[0] = new YeltPartitionIndexer(YeltPartitionReaders[0], currentKeysPtr);
                    for (int i = 1; i < indexers.Length; i++)
                        indexers[i] = new YeltPartitionIndexer(YeltPartitionReaders[i], indexers[i - 1].CurrentPosition + YeltPartitionReaders[i - 1].TotalLength);
                }

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

            return keys;
        }
     
        public void Reset()
        {
            foreach (var reader in YeltPartitionReaders)
                reader.Reset();
        }
    }
}
