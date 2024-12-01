

using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Security;

using Studio.Core;
using Arch.ILS.IntelMKL;
using static Arch.ILS.IntelMKL.SparseBlas;

namespace Arch.ILS.EconomicModel
{
    [SuppressUnmanagedCodeSecurity]
    public unsafe class YeltPartitionMapper
    {
        #region Types

        private unsafe struct MappedIndices
        {
            public MappedIndices(int** mappedIndices, int size)
            {
                Indices = mappedIndices; 
                Size = size;
            }

            public int** Indices { get; }
            public int Size{ get; }
        }

        #endregion Types

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
            int[] startIndexInMappedIndices = new int[YeltPartitionReaders.Count + 1];
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
                    int i = 1;
                    for (; i < indexers.Length; i++)
                    {
                        indexers[i] = new YeltPartitionIndexer(YeltPartitionReaders[i], indexers[i - 1].CurrentPosition + YeltPartitionReaders[i - 1].TotalLength);
                        startIndexInMappedIndices[i] = (indexerStartIndex += YeltPartitionReaders[i - 1].TotalLength);
                    }
                    startIndexInMappedIndices[i] = (indexerStartIndex += YeltPartitionReaders[i - 1].TotalLength);

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
            }
            return (mappedIndices, startIndexInMappedIndices);
        }

        private unsafe MappedIndices MapKeys2()
        {
            int keysLength = SortedKeys.Length;
            int size = YeltPartitionReaders.Count;
            int** mappedIndices = (int**)NativeMemory.AlignedAlloc((nuint)(size << 2), (nuint)Unsafe.SizeOf<IntPtr>());
            fixed (long* sortedKeys = SortedKeys)
            {
                long* currentKey = sortedKeys;
                long* lastKey = sortedKeys + keysLength - 1;
                long* end = lastKey + 1;
                YeltPartitionIndexer[] indexers = new YeltPartitionIndexer[YeltPartitionReaders.Count];

                for (int i = 0; i < indexers.Length; i++)
                {
                    mappedIndices[i] = (int*)NativeMemory.AlignedAlloc((nuint)((YeltPartitionReaders[i].TotalLength) << 2), (nuint)Unsafe.SizeOf<int>());
                    indexers[i] = new YeltPartitionIndexer(YeltPartitionReaders[i], mappedIndices[i]);
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

            return new(mappedIndices, size);
        }

        public unsafe double[] Process(double cession, int maxDegreeOfParallelism = 2)
        {
            (int[] mappedIndices, int[] startIndicesInMappedIndices) = MapKeys(); 
            Reset();
            double[] eventLosses = new double[SortedKeys.Length];

#if DEBUG
            for (int k = 0; k < YeltPartitionReaders.Count; ++k)
#else
            Parallel.For(0, YeltPartitionReaders.Count, new ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism }, k =>
#endif
            {
                Span<double> s_EventLosses = new Span<double>(eventLosses);
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
                                    Add(ref s_EventLosses[*mappedIndicesCurrentIndexPtr++], v_Cession_LossPcts[i]);
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
                                    Add(ref s_EventLosses[*mappedIndicesCurrentIndexPtr++], v_Cession_LossPcts[i]);
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
                                    Add(ref s_EventLosses[*mappedIndicesCurrentIndexPtr++], v_Cession_LossPcts[i]);
                            }
                        }

                        for (int i = nonVectorStartIndex; i < yeltPartition.CurrentLength; ++i)
                        {
                            Add(ref s_EventLosses[*mappedIndicesCurrentIndexPtr++], (*currentPtr++) * cession);
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

        public unsafe double[] ProcessB(double cession, int maxDegreeOfParallelism = 2)
        {
            MappedIndices mappedIndices = MapKeys2();
            int** mappedIndicesPtr = mappedIndices.Indices;
            int size = mappedIndices.Size;
            Reset();
            double[] eventLosses = new double[SortedKeys.Length];

#if DEBUG
            for (int k = 0; k < YeltPartitionReaders.Count; ++k)
#else
            Parallel.For(0, YeltPartitionReaders.Count, new ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism }, k =>
#endif
            {
                Span<double> s_EventLosses = new Span<double>(eventLosses);
                int* mappedIndicesCurrentIndexPtr = mappedIndicesPtr[k];
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
                                Add(ref s_EventLosses[*mappedIndicesCurrentIndexPtr++], v_Cession_LossPcts[i]);
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
                            {
                                Add(ref s_EventLosses[*mappedIndicesCurrentIndexPtr++], v_Cession_LossPcts[i]);
                            }                              
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
                            for (int i = 0; i < inVectorCount; ++i)
                                Add(ref s_EventLosses[*mappedIndicesCurrentIndexPtr++], v_Cession_LossPcts[i]);
                        }
                    }

                    for (int i = nonVectorStartIndex; i < yeltPartition.CurrentLength; ++i)
                    {
                        Add(ref s_EventLosses[*mappedIndicesCurrentIndexPtr++], (*currentPtr++) * cession);
                    }

                    yeltPartition = yeltPartition.NextNode;
                }
            }
#if !DEBUG
            );
#endif

            for (int i = 0; i < size; ++i)
            {
                NativeMemory.AlignedFree(mappedIndicesPtr[i]);
                mappedIndicesPtr[i] = null;
            }
            NativeMemory.AlignedFree(mappedIndicesPtr);
            mappedIndicesPtr = null;

            return eventLosses;
        }

        public unsafe double[] Process2(double cession, int maxDegreeOfParallelism = 2)
        {
            (int[] mappedIndices, int[] startIndicesInMappedIndices) = MapKeys();
            Reset();
            double[] losses = new double[mappedIndices.Length];

#if DEBUG
            for (int k = 0; k < YeltPartitionReaders.Count; ++k)
#else
            Parallel.For(0, YeltPartitionReaders.Count, new ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism }, k =>
#endif
            {
                fixed (int* startIndicesInMappedIndicesPtr = startIndicesInMappedIndices)
                fixed (double* eventLossesPtr = losses)
                {
                    int startIndiexInMappedIndices = *(startIndicesInMappedIndicesPtr + k);
                    double* lossesPtr = eventLossesPtr + startIndiexInMappedIndices;
                    YeltPartitionReader currentReader = YeltPartitionReaders[k];
                    YeltPartition yeltPartition = currentReader.Head;
                    while (yeltPartition != null)
                    {
                        NativeMemory.Copy(yeltPartition.CurrentStartLossPct, lossesPtr, (nuint)(yeltPartition.CurrentLength << 3));
                        lossesPtr += yeltPartition.CurrentLength;
                        yeltPartition = yeltPartition.NextNode;
                    }
                }
            }
#if !DEBUG
            );
#endif         
            transa trans = transa.T;
            GCHandle trans_pin = GCHandle.Alloc(trans, GCHandleType.Pinned);
            int count = YeltPartitionReaders.Count;
            GCHandle count_pin = GCHandle.Alloc(count, GCHandleType.Pinned);
            double[] cessions = Enumerable.Repeat(cession, count).ToArray();
            double[] eventLosses = new double[SortedKeys.Length];
            fixed (double* lossesPtr = losses)
            fixed (int* startIndicesInMappedIndicesPtr = startIndicesInMappedIndices)
            fixed (int* mappedIndicesPtr = mappedIndices)
            fixed (double* cessionsPtr = cessions)
            fixed (double* eventLossesPtr = eventLosses)
            {
                double* outputPtr = eventLossesPtr;

                Console.WriteLine($"trans_pin {trans_pin.AddrOfPinnedObject()}");
                Console.WriteLine($"count_pin {count_pin.AddrOfPinnedObject()}");
                Console.WriteLine($"lossesPtr {(nuint)lossesPtr}");
                Console.WriteLine($"startIndicesInMappedIndicesPtr {(nuint)startIndicesInMappedIndicesPtr}");
                Console.WriteLine($"mappedIndicesPtr {(nuint)mappedIndicesPtr}");
                Console.WriteLine($"cessionsPtr {(nuint)cessionsPtr}");
                Console.WriteLine($"outputPtr {(nuint)outputPtr}");


                Console.WriteLine($"trans_pinPtrAddress {GCHandle.ToIntPtr(trans_pin)}");
                Console.WriteLine($"count_pinPtrAddress {GCHandle.ToIntPtr(count_pin)}");
                Console.WriteLine($"lossesPtrPtr {(nuint)(&lossesPtr)}");
                Console.WriteLine($"startIndicesInMappedIndicesPtrPtr {(nuint)(&startIndicesInMappedIndicesPtr)}");
                Console.WriteLine($"mappedIndicesPtrPtr {(nuint)(&mappedIndicesPtr)}");
                Console.WriteLine($"cessionsPtrPtr {(nuint)(&cessionsPtr)}");
                Console.WriteLine($"outputPtrPtr {(nuint)(&outputPtr)}");
                Console.WriteLine($"outputPtr {(nuint)(&eventLossesPtr)}");

                mkl_cspblas_dcsrgemv(ref trans, ref count, losses, startIndicesInMappedIndices, mappedIndices, cessions, eventLosses);

            }
            double sum = eventLosses.Select((x, i) => (x, i)).Where(ii => ii.i > 315).Sum(s => s.x);
            var zz = mappedIndices.Where(x => x == 0).ToList();
            var zzz = mappedIndices.Select((x, i) => (x, i)).Where(xx => xx.x == 0).ToList();

            GCHandle losses_pin = GCHandle.Alloc(losses, GCHandleType.Pinned);
            GCHandle startIndicesInMappedIndices_pin = GCHandle.Alloc(startIndicesInMappedIndices, GCHandleType.Pinned);
            GCHandle mappedIndices_pin = GCHandle.Alloc(mappedIndices, GCHandleType.Pinned);
            GCHandle cessions_pin = GCHandle.Alloc(cessions, GCHandleType.Pinned);
            //GCHandle eventLosses_pin = GCHandle.Alloc(eventLosses, GCHandleType.Pinned);
            //double* eventLossesPtr2 = (double*)NativeMemory.AlignedAlloc((nuint)(SortedKeys.Length << 3), (nuint)Unsafe.SizeOf<double>());
            Console.WriteLine($"trans_pin {trans_pin.AddrOfPinnedObject()}");
            Console.WriteLine($"count_pin {count_pin.AddrOfPinnedObject()}");
            Console.WriteLine($"losses_pin {losses_pin.AddrOfPinnedObject()}");
            Console.WriteLine($"startIndicesInMappedIndices_pin {startIndicesInMappedIndices_pin.AddrOfPinnedObject()}");
            Console.WriteLine($"mappedIndices_pin {mappedIndices_pin.AddrOfPinnedObject()}");
            Console.WriteLine($"cessions_pin {cessions_pin.AddrOfPinnedObject()}");
            //Console.WriteLine($"eventLosses_pin {eventLosses_pin.AddrOfPinnedObject()}");
            mkl_cspblas_dcsrgemv2(ref trans, ref count, losses, startIndicesInMappedIndices, mappedIndices, cessions, eventLosses);
            trans_pin.Free();
            count_pin.Free();
            losses_pin.Free();
            startIndicesInMappedIndices_pin.Free();
            mappedIndices_pin.Free();
            cessions_pin.Free();
            //NativeMemory.Free(eventLossesPtr2);
            //eventLosses_pin.Free();
            Console.WriteLine(eventLosses.Sum());
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
