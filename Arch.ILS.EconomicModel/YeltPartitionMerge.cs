
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

using Arch.ILS.Core;

namespace Arch.ILS.EconomicModel
{
    public class YeltPartitionMerge
    {
        public static unsafe int Merge_ScalarOptimised_2(YeltPartition partitionsA, YeltPartition partitionsB, long* dst)
        {
            int i = 0, j = 0, k = 0;
            YeltPartition currentPartitionA = partitionsA;
            YeltPartition currentPartitionB = partitionsB;
            long* aArr = currentPartitionA.CurrentStartKey;
            long* bArr = currentPartitionB.CurrentStartKey;
            int aCnt = currentPartitionA.CurrentLength;
            int bCnt = currentPartitionB.CurrentLength;

            while (currentPartitionA != null && currentPartitionB != null)
            {
                aArr = currentPartitionA.CurrentStartKey;
                bArr = currentPartitionB.CurrentStartKey;
                aCnt = currentPartitionA.CurrentLength;
                bCnt = currentPartitionB.CurrentLength;

                while (i < aCnt - 32 && j < bCnt - 32)
                {
                    for (int t = 0; t < 32; t++)
                    {
                        long a = aArr[i], b = bArr[j];
                        dst[k++] = a < b ? a : b;
                        i += (a <= b) ? 1 : 0;
                        j += (a >= b) ? 1 : 0;
                    }
                }

                while (i < aCnt && j < bCnt)
                {
                    long a = aArr[i], b = bArr[j];
                    dst[k++] = a < b ? a : b;
                    i += (a <= b) ? 1 : 0;
                    j += (a >= b) ? 1 : 0;
                }

                currentPartitionA = (i == aCnt) ? currentPartitionA.NextNode : currentPartitionA;
                currentPartitionB = (j == bCnt) ? currentPartitionB.NextNode : currentPartitionB;
                i = (i == aCnt) ? 0 : i;
                j = (j == bCnt) ? 0 : j;
            }

            aCnt = currentPartitionA == null ? 0 : aCnt;
            bCnt = currentPartitionB == null ? 0 : bCnt;
            Unsafe.CopyBlock(dst + k, aArr + i, (uint)(aCnt - i) * sizeof(long));
            k += (aCnt - i);
            Unsafe.CopyBlock(dst + k, bArr + j, (uint)(bCnt - j) * sizeof(long));
            k += (bCnt - j);

            currentPartitionA = currentPartitionA?.NextNode ?? null;
            while (currentPartitionA != null)
            {
                aArr = currentPartitionA.CurrentStartKey;
                aCnt = currentPartitionA.CurrentLength;
                Unsafe.CopyBlock(dst + k, aArr, (uint)aCnt * sizeof(long));
                k += aCnt;
                currentPartitionA = currentPartitionA.NextNode;
            }


            currentPartitionB = currentPartitionB?.NextNode ?? null;
            while (currentPartitionB != null)
            {
                bArr = currentPartitionB.CurrentStartKey;
                bCnt = currentPartitionB.CurrentLength;
                Unsafe.CopyBlock(dst + k, bArr, (uint)bCnt * sizeof(long));
                k += bCnt;
                currentPartitionB = currentPartitionB.NextNode;
            }

            return k;
        }

        public static unsafe int Merge_ScalarOptimised_2(YeltPartition partitions, long* dst)
        {
            int k = 0;
            YeltPartition currentPartition = partitions;
            long* aArr = currentPartition.CurrentStartKey;
            int aCnt = currentPartition.CurrentLength;

            while (currentPartition != null)
            {
                aArr = currentPartition.CurrentStartKey;
                aCnt = currentPartition.CurrentLength;
                Unsafe.CopyBlock(dst + k, aArr, (uint)aCnt * sizeof(long));
                k += aCnt;
                currentPartition = currentPartition.NextNode;
            }

            return k;
        }

        public static unsafe long[] Merge(IEnumerable<YeltPartitionReader> readers)
        {
            YeltPartitionReader[] orderedReaders = readers.OrderByDescending(x => x.TotalLength).ToArray();
            int mergeCount = (orderedReaders.Length >> 1) + ((orderedReaders.Length % 2) == 0 ? 0 : 1);
            long[][] tempResult = new long[mergeCount][];
            int[] tempSizes = new int[mergeCount];
            int i = 1;
            for (; i < orderedReaders.Length; i += 2)
            {
                var readerA = orderedReaders[i - 1];
                var readerB = orderedReaders[i];
                long[] sortedKeys = new long[readerA.TotalLength + readerB.TotalLength];
                fixed (long* keysPtr = sortedKeys)
                {
                    long* ptr = keysPtr;
                    tempSizes[(i >> 1)] = Merge_ScalarOptimised_2(readerA.Head, readerB.Head, ptr);
                }
                tempResult[(i >> 1)] = sortedKeys;
            }

            i -= 1;
            if (i < orderedReaders.Length)
            {
                var reader = orderedReaders[i];
                long[] sortedKeys = new long[reader.TotalLength];
                fixed (long* keysPtr = sortedKeys)
                {
                    long* ptr = keysPtr;
                    tempSizes[mergeCount - 1] = Merge_ScalarOptimised_2(reader.Head, ptr);
                }
                tempResult[mergeCount - 1] = sortedKeys;
            }

            long[][] currentResult = tempResult;
            int[] currentSizes = tempSizes;
            mergeCount = (mergeCount >> 1);
            while (mergeCount > 0)
            {
                tempResult = new long[mergeCount][];
                tempSizes = new int[mergeCount];
                for (int j = 1; j < currentResult.Length; j += 2)
                {
                    var resultA = currentResult[j - 1];
                    var resultB = currentResult[j];
                    int sizeA = currentSizes[j - 1];
                    int sizeB = currentSizes[j];
                    long[] sortedKeys = new long[sizeA + sizeB];
                    fixed (long* keysA = resultA)
                    fixed (long* keysB = resultB)
                    fixed (long* keysPtr = sortedKeys)
                    {
                        long* ptr = keysPtr;
                        tempSizes[(j >> 1)] = MergeRemoveDuplicate.Merge_ScalarOptimised_2(keysA, sizeA, keysB, sizeB, ptr);
                    }
                    tempResult[(j >> 1)] = sortedKeys;
                }
                currentResult = tempResult;
                currentSizes = tempSizes;
                mergeCount = (mergeCount >> 1);
            }

            Array.Resize(ref currentResult[0], currentSizes[0]);
            return currentResult[0];
        }

        public static unsafe long[] Merge_Native(IEnumerable<YeltPartitionReader> readers)
        {
            YeltPartitionReader[] orderedReaders = readers.OrderByDescending(x => x.TotalLength).ToArray();
            int tempMergeCount = (orderedReaders.Length >> 1) + ((orderedReaders.Length % 2) == 0 ? 0 : 1);
            long** tempResult = (long**)NativeMemory.AlignedAlloc((nuint)(Unsafe.SizeOf<IntPtr>() * tempMergeCount), (nuint)Unsafe.SizeOf<IntPtr>());
            int* tempSizes = (int*)NativeMemory.AlignedAlloc((nuint)(tempMergeCount << 2), (nuint)Unsafe.SizeOf<int>());
            int i = 1;
            for (; i < orderedReaders.Length; i += 2)
            {
                var readerA = orderedReaders[i - 1];
                var readerB = orderedReaders[i];
                long* sortedKeys = (long*)NativeMemory.AlignedAlloc((nuint)((readerA.TotalLength + readerB.TotalLength) << 3), (nuint)Unsafe.SizeOf<long>());
                tempSizes[(i >> 1)] = Merge_ScalarOptimised_2(readerA.Head, readerB.Head, sortedKeys);
                tempResult[(i >> 1)] = sortedKeys;
            }

            i -= 1;
            if (i < orderedReaders.Length)
            {
                var reader = orderedReaders[i];
                long* sortedKeys = (long*)NativeMemory.AlignedAlloc((nuint)(reader.TotalLength << 3), (nuint)Unsafe.SizeOf<long>());
                tempSizes[tempMergeCount - 1] = Merge_ScalarOptimised_2(reader.Head, sortedKeys);
                tempResult[tempMergeCount - 1] = sortedKeys;
            }

            long** currentResult = tempResult;
            int* currentSizes = tempSizes;
            int mergeCount = tempMergeCount;
            tempMergeCount = (tempMergeCount >> 1);
            while (tempMergeCount > 0)
            {
                tempResult = (long**)NativeMemory.AlignedAlloc((nuint)(Unsafe.SizeOf<IntPtr>() * tempMergeCount), (nuint)Unsafe.SizeOf<IntPtr>());
                tempSizes = (int*)NativeMemory.AlignedAlloc((nuint)(tempMergeCount << 2), (nuint)Unsafe.SizeOf<int>());
                for (int j = 1; j < mergeCount; j += 2)
                {
                    long* keysA = currentResult[j - 1];
                    long* keysB = currentResult[j];
                    int sizeA = currentSizes[j - 1];
                    int sizeB = currentSizes[j];
                    long* sortedKeys = (long*)NativeMemory.AlignedAlloc((nuint)((sizeA + sizeB) << 3), (nuint)Unsafe.SizeOf<long>());
                    tempSizes[(j >> 1)] = MergeRemoveDuplicate.Merge_ScalarOptimised_2(keysA, sizeA, keysB, sizeB, sortedKeys);
                    tempResult[(j >> 1)] = sortedKeys;
                }

                for (int j = 0; j < mergeCount; j++)
                {
                    if (currentResult[j] != null)
                    {
                        NativeMemory.AlignedFree(currentResult[j]);
                        currentResult[j] = null;
                    }
                }

                NativeMemory.AlignedFree(currentResult);
                currentResult = tempResult;
                tempResult = null;

                NativeMemory.AlignedFree(currentSizes);
                currentSizes = tempSizes;
                tempSizes = null;

                mergeCount = tempMergeCount;
                tempMergeCount = (tempMergeCount >> 1);
            }

            int size = currentSizes[0];
            long[] result = new long[size];
            fixed(long* resultPtr = result)
            {
                long* ptr = resultPtr;
                NativeMemory.Copy(currentResult[0], ptr, (nuint)(size << 3));
            }

            NativeMemory.AlignedFree(currentResult[0]);
            currentResult[0] = null;
            NativeMemory.AlignedFree(currentResult);
            NativeMemory.AlignedFree(currentSizes);

            return result;
        }
    }
}
