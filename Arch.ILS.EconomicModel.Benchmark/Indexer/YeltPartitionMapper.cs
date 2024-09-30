
namespace Arch.ILS.EconomicModel.Benchmark
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

        public unsafe int[] MapKeysFromDictionary()
        {
            int[] keys = new int[TotalLength];
            int*[] indices = new int*[YeltPartitionReaders.Count];
            Dictionary<long, int> keyIndexMapper = SortedKeys.Select((x, i) => (x, i)).ToDictionary(kv => kv.x, kv=> kv.i);
            fixed (int* keysPtr = keys)
            {
                int* currentKeysPtr = keysPtr;
                indices[0] = currentKeysPtr;
                for (int i = 1; i < indices.Length; i++)
                    indices[i] = indices[i - 1] + YeltPartitionReaders[i - 1].TotalLength;

                Parallel.For(0, YeltPartitionReaders.Count, i => {
                    YeltPartitionReader reader = YeltPartitionReaders[i];
                    int* currentIndex = indices[i];
                    do
                    {
                        *currentIndex++ = keyIndexMapper[*reader.CurrentPartitionCurrentItem];
                    }
                    while (reader.SetNext());
                });
            }

            return keys;
        }

        public unsafe int[] MapKeysFromFixedDirectory()
        {
            const int Size = FixedDirectory<Int64Span, YeltPartitionIndexer>.MaxCount;
            var defaultEntry = FixedDirectory<Int64Span, YeltPartitionIndexer>.Default;
            int keysLength = SortedKeys.Length;
            int[] keys = new int[TotalLength];
            fixed (long* sortedKeys = SortedKeys)
            {
                long* currentKey = sortedKeys;
                long* lastKey = sortedKeys + keysLength - 1;
                long* lastBatchStartKey = lastKey - Size + 1;
                int currentKeyIndex = 0;
                YeltPartitionIndexer[] indexers = new YeltPartitionIndexer[YeltPartitionReaders.Count];
                fixed (int* keysPtr = keys)
                {
                    int* currentKeysPtr = keysPtr;
                    indexers[0] = new YeltPartitionIndexer(YeltPartitionReaders[0], currentKeysPtr);
                    for (int i = 1; i < indexers.Length; i++)
                        indexers[i] = new YeltPartitionIndexer(YeltPartitionReaders[i], indexers[i - 1].CurrentPosition + YeltPartitionReaders[i - 1].TotalLength);
                }

                while (currentKey <= lastBatchStartKey)
                {
                    FixedDirectory<Int64Span, YeltPartitionIndexer> keyIndexMapper = new();
                    for (int r = 0; r < indexers.Length; r++)
                    {
                        YeltPartitionIndexer indexer = indexers[r];
                        if(indexer.YeltPartitionReader.IsOpen)
                            keyIndexMapper.Add(new Int64Span(indexer.YeltPartitionReader.CurrentPartitionCurrentItem), ref indexer);
                    }
                    int i = 0;

                    while (i < Size)
                    {
                        Int64Span key = new Int64Span(currentKey);
                        ref var entry = ref keyIndexMapper.GetFirstRef(ref key);

                        while(!entry.Equals(defaultEntry))
                        {
                            YeltPartitionIndexer indexer = entry.value;
                            YeltPartitionReader reader = indexer.YeltPartitionReader;
                            do
                            {
                                if(*reader.CurrentPartitionCurrentItem == *currentKey)
                                    indexer.Write(ref currentKeyIndex);
                                else
                                {
                                    if(reader.IsOpen)
                                    {
                                        if (++i >= Size)
                                            goto nextBatch;
                                        keyIndexMapper.Add(new Int64Span(reader.CurrentPartitionCurrentItem), ref indexer);
                                    }
                                    break;
                                }

                            }
                            while (reader.SetNext());

                            ref var tempEntry = ref keyIndexMapper.GetNextRef(ref key, ref entry);
                            entry = ref tempEntry;
                        }

                        currentKey++;
                        currentKeyIndex++;
                        i++;
                    }

                nextBatch:;
                }

                var end = lastKey + 1; 
                FixedDirectory<Int64Span, YeltPartitionIndexer> lastKeyIndexMapper = new();
                for (int r = 0; r < indexers.Length; r++)
                {
                    YeltPartitionIndexer indexer = indexers[r];
                    if (indexer.YeltPartitionReader.IsOpen)
                        lastKeyIndexMapper.Add(new Int64Span(indexer.YeltPartitionReader.CurrentPartitionCurrentItem), ref indexer);
                }

                while (currentKey != end)
                {
                    Int64Span key = new Int64Span(currentKey);
                    ref var entry = ref lastKeyIndexMapper.GetFirstRef(ref key);

                    while (!entry.Equals(defaultEntry))
                    {
                        YeltPartitionIndexer indexer = entry.value;
                        YeltPartitionReader reader = indexer.YeltPartitionReader;
                        do
                        {
                            if (*reader.CurrentPartitionCurrentItem == *currentKey)
                                indexer.Write(ref currentKeyIndex);
                            else
                            {
                                if (reader.IsOpen)
                                    lastKeyIndexMapper.Add(new Int64Span(reader.CurrentPartitionCurrentItem), ref indexer);
                                break;
                            }

                        }
                        while (reader.SetNext());

                        ref var tempEntry = ref lastKeyIndexMapper.GetNextRef(ref key, ref entry);
                        entry = ref tempEntry;
                    }

                    currentKey++;
                    currentKeyIndex++;
                }
            }

            return keys;
        }

        public unsafe int[] MapKeysFromDynamicDirectory()
        {
            var defaultEntry = DynamicDirectory<Int64Span, YeltPartitionIndexer>.Default;
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

                DynamicDirectory<Int64Span, YeltPartitionIndexer> keyIndexMapper = new(YeltPartitionReaders.Count);
                for (int r = 0; r < indexers.Length; r++)
                {
                    YeltPartitionIndexer indexer = indexers[r];
                    if (indexer.YeltPartitionReader.IsOpen)
                        keyIndexMapper.Add(new Int64Span(indexer.YeltPartitionReader.CurrentPartitionCurrentItem), ref indexer);
                }

                while (currentKey != end)
                {
                    Int64Span key = new Int64Span(currentKey);
                    ref var entry = ref keyIndexMapper.GetFirstRef(ref key);

                    while (!entry.Equals(defaultEntry))
                    {
                        YeltPartitionIndexer indexer = entry.value;
                        YeltPartitionReader reader = indexer.YeltPartitionReader;

                        if (*reader.CurrentPartitionCurrentItem == *currentKey)
                        {
                            indexer.Write(ref currentKeyIndex);
                            ref var tempEntry = ref keyIndexMapper.GetNextRef(ref key, ref entry);
                            if (reader.SetNext())
                                keyIndexMapper.Move(key, new Int64Span(reader.CurrentPartitionCurrentItem), ref entry);
                            entry = ref tempEntry;
                        }
                        else
                        {
                            ref var tempEntry = ref keyIndexMapper.GetNextRef(ref key, ref entry);
                            entry = ref tempEntry;
                        }
                    }

                    currentKey++;
                    currentKeyIndex++;
                }
            }

            return keys;
        }

        public unsafe int[] MapKeysFromDynamicDirectory2()
        {
            var defaultEntry = DynamicDirectory2<Int64Span, YeltPartitionIndexer>.Default;
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

                DynamicDirectory2<Int64Span, YeltPartitionIndexer> keyIndexMapper = new(YeltPartitionReaders.Count);
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
                    ref var entry = ref keyIndexMapper.GetFirstRef(ref key);

                    while (!entry.Equals(defaultEntry))
                    {
                        YeltPartitionIndexer indexer = entry.value;
                        YeltPartitionReader reader = indexer.YeltPartitionReader;

                        if (*reader.CurrentPartitionCurrentItem == *currentKey)
                        {
                            indexer.Write(ref currentKeyIndex);
                            ref var tempEntry = ref keyIndexMapper.GetNextRef(ref key, ref entry);
                            if (reader.SetNext())
                            {
                                var newKey = new Int64Span(reader.CurrentPartitionCurrentItem);
                                keyIndexMapper.Move(ref key, ref newKey, ref entry);
                            }
                            entry = ref tempEntry;
                        }
                        else
                        {
                            ref var tempEntry = ref keyIndexMapper.GetNextRef(ref key, ref entry);
                            entry = ref tempEntry;
                        }
                    }

                    currentKey++;
                    currentKeyIndex++;
                }
            }

            return keys;
        }

        public unsafe int[] MapKeysFromDynamicDirectory3()
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

                DynamicDirectory3<Int64Span, YeltPartitionIndexer> keyIndexMapper = new(YeltPartitionReaders.Count);
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
                        YeltPartitionIndexer indexer = entry.value;
                        YeltPartitionReader reader = indexer.YeltPartitionReader;

                        if (*reader.CurrentPartitionCurrentItem == *currentKey)
                        {
                            indexer.Write(ref currentKeyIndex);
                            var tempEntry = keyIndexMapper.GetNextRef(ref key, ref entry);
                            if (reader.SetNext())
                            {
                                var newKey = new Int64Span(reader.CurrentPartitionCurrentItem);
                                keyIndexMapper.Move(ref key, ref newKey, ref entry);
                            }
                            entry = tempEntry;
                        }
                        else
                        {
                            var tempEntry = keyIndexMapper.GetNextRef(ref key, ref entry);
                            entry = tempEntry;
                        }
                    }

                    currentKey++;
                    currentKeyIndex++;
                }
            }

            return keys;
        }

        public unsafe int[] MapKeysFromDynamicDirectory5()
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

                using (DynamicDirectory5<Int64Span, YeltPartitionIndexer> keyIndexMapper = new(YeltPartitionReaders.Count))
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
