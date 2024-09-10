using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Arch.ILS.EconomicModel
{
    public class YeltPartitionMerge
    {
        public static unsafe int Merge_ScalarOptimised_2(YeltPartitionLinkedList partitionsA, YeltPartitionLinkedList partitionsB, long* dst)
        {
            int i = 0, j = 0, k = 0;
            YeltPartitionLinkedList currentPartitionA = partitionsA;
            YeltPartitionLinkedList currentPartitionB = partitionsB;
            long* aArr = currentPartitionA.CurrentStartKey;
            long* bArr = currentPartitionB.CurrentStartKey;
            int aCnt = currentPartitionA.CurrentLength;
            int bCnt = currentPartitionB.CurrentLength;

            while (currentPartitionA != null && currentPartitionB != null)
            {
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
                aArr = (i == aCnt) ? currentPartitionA.CurrentStartKey : aArr;
                bArr = (j == bCnt) ? currentPartitionB.CurrentStartKey : bArr;
                aCnt = (i == aCnt) ? currentPartitionA.CurrentLength : aCnt;
                bCnt = (j == bCnt) ? currentPartitionB.CurrentLength : bCnt;
                i = (i == aCnt) ? 0 : i;
                j = (j == bCnt) ? 0 : j;
            } 

            Unsafe.CopyBlock(dst + k, aArr + i, (uint)(aCnt - i) * sizeof(long));
            k += (aCnt - i);
            Unsafe.CopyBlock(dst + k, bArr + j, (uint)(bCnt - j) * sizeof(long));
            k += (bCnt - j);

            currentPartitionA = currentPartitionA.NextNode;
            while (currentPartitionA != null)
            {
                aArr = currentPartitionA.CurrentStartKey;
                aCnt = currentPartitionA.CurrentLength;
                Unsafe.CopyBlock(dst + k, aArr, (uint)aCnt * sizeof(long));
                k += aCnt;
                currentPartitionA = currentPartitionA.NextNode;
            }


            currentPartitionB = currentPartitionB.NextNode;
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
    }
}
