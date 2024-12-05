
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Security;

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
            public int Size { get; }
        }

        #endregion Types

        #region Constants

        public const int DefaultBufferSize = 1024;
        public const int DefaultPartitionCount = 8;

        #endregion Constants

        #region Variables

        private Dictionary<long, int> _keyIndices;

        #endregion Variables

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

        public unsafe (int[] MappedIndices, int[] StartIndicesInMappedIndices) MapKeys(int partitionCount = DefaultPartitionCount)
        {
            int keysLength = SortedKeys.Length;
            Range[] ranges = new Range[partitionCount];
            int partitionSize = YeltPartitionReaders.Count / partitionCount;
            int currentPartitionIndex = 0;
            for (int i = 0; i < partitionCount - 1; i++)
                ranges[i] = new Range(currentPartitionIndex, currentPartitionIndex += partitionSize);
            ranges[partitionCount - 1] = new Range(currentPartitionIndex, YeltPartitionReaders.Count);
            int[] mappedIndices = new int[TotalLength];
            int[] startIndexInMappedIndices = new int[YeltPartitionReaders.Count + 1];

            int indexerStartIndex = 0;
            startIndexInMappedIndices[0] = indexerStartIndex;
            for (int k = 1; k < startIndexInMappedIndices.Length; ++k)
                startIndexInMappedIndices[k] = (indexerStartIndex += YeltPartitionReaders[k - 1].TotalLength);

            Task[] tasks = new Task[partitionCount];
            for (int partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex)
            {
                tasks[partitionIndex] = Task.Factory.StartNew((rangeObj) =>
                {
                    Range range = (Range)rangeObj;
                    fixed (long* sortedKeys = SortedKeys)
                    {
                        long* currentKey = sortedKeys;
                        long* lastKey = sortedKeys + keysLength - 1;
                        var end = lastKey + 1;

                        int readerStartIndex = range.Start.Value;
                        int readerEndIndex = range.End.Value;
                        int readersCount = readerEndIndex - readerStartIndex;
                        YeltPartitionIndexer[] indexers = new YeltPartitionIndexer[readersCount];

                        fixed (int* keysPtr = mappedIndices)
                        {
                            int* currentKeysPtr = keysPtr + startIndexInMappedIndices[readerStartIndex];
                            int i = readerStartIndex, j = 0;
                            indexers[j] = new YeltPartitionIndexer(YeltPartitionReaders[i], currentKeysPtr);
                            for (++j, ++i; i < readerEndIndex; ++i, ++j)
                                indexers[j] = new YeltPartitionIndexer(YeltPartitionReaders[i], indexers[j - 1].CurrentPosition + YeltPartitionReaders[i - 1].TotalLength);

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
                }, ranges[partitionIndex]);
            }
            Task.WaitAll(tasks);
            return (mappedIndices, startIndexInMappedIndices);
        }

        private unsafe MappedIndices MapPartitionedKeys(int partitionCount = DefaultPartitionCount)
        {
            int keysLength = SortedKeys.Length;
            int size = YeltPartitionReaders.Count;
            Range[] ranges = new Range[partitionCount];
            int partitionSize = YeltPartitionReaders.Count / partitionCount;
            int currentPartitionIndex = 0;
            for (int i = 0; i < partitionCount - 1; i++)
                ranges[i] = new Range(currentPartitionIndex, currentPartitionIndex += partitionSize);
            ranges[partitionCount - 1] = new Range(currentPartitionIndex, YeltPartitionReaders.Count);
            int** mappedIndices = (int**)NativeMemory.AlignedAlloc((nuint)(Unsafe.SizeOf<IntPtr>() * size), (nuint)Unsafe.SizeOf<IntPtr>());

            Task[] tasks = new Task[partitionCount];
            for (int partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex)
            {
                tasks[partitionIndex] = Task.Factory.StartNew((rangeObj) =>
                {
                    Range range = (Range)rangeObj;
                    fixed (long* sortedKeys = SortedKeys)
                    {
                        long* currentKey = sortedKeys;
                        long* lastKey = sortedKeys + keysLength - 1;
                        long* end = lastKey + 1;
                        int readerStartIndex = range.Start.Value;
                        int readerEndIndex = range.End.Value;
                        int readersCount = readerEndIndex - readerStartIndex;
                        YeltPartitionIndexer[] indexers = new YeltPartitionIndexer[readersCount];
                        for (int i = readerStartIndex, j = 0; i < readerEndIndex; i++, j++)
                        {
                            mappedIndices[i] = (int*)NativeMemory.AlignedAlloc((nuint)((YeltPartitionReaders[i].TotalLength) << 2), (nuint)Unsafe.SizeOf<int>());
                            indexers[j] = new YeltPartitionIndexer(YeltPartitionReaders[i], mappedIndices[i]);
                        }

                        int currentKeyIndex = 0;
                        using (DynamicDirectory<Int64Span, YeltPartitionIndexer> keyIndexMapper = new(readersCount))
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
                }, ranges[partitionIndex]);
            }
            Task.WaitAll(tasks);
            return new(mappedIndices, size);
        }
        private unsafe MappedIndices MapPartitionedKeysBasic(int partitionCount = DefaultPartitionCount)
        {
            if (_keyIndices == null)
                _keyIndices = SortedKeys.Select((x, i) => (x, i)).ToDictionary(k => k.x, v => v.i);

            int keysLength = SortedKeys.Length;
            int size = YeltPartitionReaders.Count;
            int** mappedIndices = (int**)NativeMemory.AlignedAlloc((nuint)(Unsafe.SizeOf<IntPtr>() * size), (nuint)Unsafe.SizeOf<IntPtr>());
            Parallel.For(0, size, (i) => 
            {
                mappedIndices[i] = (int*)NativeMemory.AlignedAlloc((nuint)((YeltPartitionReaders[i].TotalLength) << 2), (nuint)Unsafe.SizeOf<int>());
                var reader = YeltPartitionReaders[i];
                var indexer = new YeltPartitionIndexer(YeltPartitionReaders[i], mappedIndices[i]);

                if(indexer.YeltPartitionReader.IsOpen)
                {
                    do
                    {
                        indexer.Write(_keyIndices[*indexer.YeltPartitionReader.CurrentPartitionCurrentItem]);
                        reader.SetNext();
                    } 
                    while (indexer.YeltPartitionReader.IsOpen);
                }
            });
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
            }
#if !DEBUG
            );
#endif


            return eventLosses;
        }

        public unsafe double[] ProcessPartitions(double cession, int maxDegreeOfParallelism = 2)
        {
            MappedIndices mappedIndices = MapPartitionedKeys();
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

        public unsafe double[] ProcessNative(double cession, int maxDegreeOfParallelism = 2)
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
                    int startIndexInMappedIndices = *(startIndicesInMappedIndicesPtr + k);
                    double* lossesPtr = eventLossesPtr + startIndexInMappedIndices;
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

            int columns = YeltPartitionReaders.Count;
            double[] cessions = Enumerable.Repeat(cession, columns).ToArray();
            double[] eventLosses = new double[SortedKeys.Length];
            //double* eventLossesPtr2 = (double*)NativeMemory.AlignedAlloc((nuint)(SortedKeys.Length << 3), (nuint)Unsafe.SizeOf<double>());
            fixed (double* lossesPtr = losses)
            fixed (int* startIndicesInMappedIndicesPtr = startIndicesInMappedIndices)
            fixed (int* mappedIndicesPtr = mappedIndices)
            fixed (double* cessionsPtr = cessions)
            fixed (double* eventLossesPtr = eventLosses)
            {
                GCHandle columns_pin = GCHandle.Alloc(columns, GCHandleType.Pinned);
                sparse_index_base_t indexing = sparse_index_base_t.SPARSE_INDEX_BASE_ZERO;
                GCHandle indexing_pin = GCHandle.Alloc(indexing, GCHandleType.Pinned);
                int rows = SortedKeys.Length;
                GCHandle rows_pin = GCHandle.Alloc(rows, GCHandleType.Pinned);
                IntPtr A = new IntPtr();
                sparse_status_t csc_creation_status = mkl_sparse_d_create_csc(&A, indexing, rows, columns, startIndicesInMappedIndicesPtr, startIndicesInMappedIndicesPtr + 1, mappedIndicesPtr, lossesPtr);
                if (csc_creation_status != sparse_status_t.SPARSE_STATUS_SUCCESS)
                    throw new MklException("mkl_sparse_d_create_csc");
                sparse_operation_t operation = sparse_operation_t.SPARSE_OPERATION_NON_TRANSPOSE;
                GCHandle operation_pin = GCHandle.Alloc(operation, GCHandleType.Pinned);
                double alpha = 1;
                GCHandle alpha_pin = GCHandle.Alloc(alpha, GCHandleType.Pinned);
                matrix_descr matrix_descr = new matrix_descr { sparse_matrix_type_t = sparse_matrix_type_t.SPARSE_MATRIX_TYPE_GENERAL, sparse_fill_mode_t = sparse_fill_mode_t.SPARSE_FILL_MODE_FULL, sparse_diag_type_t = sparse_diag_type_t.SPARSE_DIAG_NON_UNIT };
                GCHandle matrix_descr_pin = GCHandle.Alloc(matrix_descr, GCHandleType.Pinned);
                double beta = 1;
                GCHandle beta_pin = GCHandle.Alloc(beta, GCHandleType.Pinned);
                sparse_status_t mv_status = mkl_sparse_d_mv(operation, alpha, A, matrix_descr, cessionsPtr, beta, eventLossesPtr);
                if (mv_status != sparse_status_t.SPARSE_STATUS_SUCCESS)
                    throw new MklException("mkl_sparse_d_mv");
                sparse_status_t destroy_status = mkl_sparse_destroy(A);
                if (destroy_status != sparse_status_t.SPARSE_STATUS_SUCCESS)
                    throw new MklException("mkl_sparse_destroy");
                /*mkl_cspblas_dcsrgemv not recommanded for large matrix. Throws access violation when large matrix*/
                //transa trans = transa.T;
                //GCHandle trans_pin = GCHandle.Alloc(trans, GCHandleType.Pinned);
                //mkl_cspblas_dcsrgemv(ref trans, ref columns, losses, startIndicesInMappedIndices, mappedIndices, cessions, eventLosses);
                indexing_pin.Free();
                rows_pin.Free();
                operation_pin.Free();
                alpha_pin.Free();
                matrix_descr_pin.Free();
                beta_pin.Free();
                columns_pin.Free();
                //trans_pin.Free();
            }
            return eventLosses;
        }

        public unsafe double[] ProcessPartitionsNative(double cession)
        {
            MappedIndices mappedIndices = MapPartitionedKeys();
            int** mappedIndicesPtr = mappedIndices.Indices;
            int size = mappedIndices.Size;
            Reset();
            double[] eventLosses = new double[SortedKeys.Length];
            int columns = 1;
            GCHandle columns_pin = GCHandle.Alloc(columns, GCHandleType.Pinned);
            transa trans = transa.T;
            GCHandle trans_pin = GCHandle.Alloc(trans, GCHandleType.Pinned);
            int[] startIndicesInMappedIndices = new int[2];
            double[] cessions = new double[1];
            fixed (int* startIndicesInMappedIndicesPtr = startIndicesInMappedIndices)
            fixed (double* eventLossesPtr = eventLosses)
            fixed (double* cessionsPtr = cessions)
            {
                int* startIndicesPtr = startIndicesInMappedIndicesPtr;
                double* destinationLossesPtr = eventLossesPtr;
                double* cessionPtr = cessionsPtr;
                for (int k = 0; k < YeltPartitionReaders.Count; ++k)
                {
                    *cessionPtr = 1;
                    int* mappedIndicesCurrentIndexPtr = mappedIndicesPtr[k];
                    YeltPartitionReader currentReader = YeltPartitionReaders[k];
                    YeltPartition yeltPartition = currentReader.Head;
                    //int rows = SortedKeys.Length;
                    //GCHandle rows_pin = GCHandle.Alloc(rows, GCHandleType.Pinned);
                    while (yeltPartition != null)
                    {
                        *(startIndicesPtr + 1) = yeltPartition.CurrentLength;
                        mkl_cspblas_dcsrgemv_ptr(ref trans, ref columns, yeltPartition.CurrentStartLossPct, startIndicesPtr, mappedIndicesCurrentIndexPtr, cessionPtr, destinationLossesPtr);
                        mappedIndicesCurrentIndexPtr += yeltPartition.CurrentLength;
                        yeltPartition = yeltPartition.NextNode;
                    }
                }
            }

            columns_pin.Free();
            //rows_pin.Free();
            trans_pin.Free();

            for (int i = 0; i < size; ++i)
            {
                NativeMemory.AlignedFree(mappedIndicesPtr[i]);
                mappedIndicesPtr[i] = null;
            }
            NativeMemory.AlignedFree(mappedIndicesPtr);
            mappedIndicesPtr = null;

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
