
using System.Diagnostics;

namespace Arch.ILS.EconomicModel.Benchmark.Indexer
{
    internal class IndexerCheckTest
    {
        public static unsafe void Execute()
        {
            MockRevoLayerDayYeltRepository revoLayerDayYeltRepository = new MockRevoLayerDayYeltRepository(10000000);
            var layerYelt0 = revoLayerDayYeltRepository.GetLayerYelt0().Result;
            Range[] ranges = [new Range(2, 50)];
            YeltPartitioner yeltPartitioner0 = new YeltPartitioner(ranges, layerYelt0);
            YeltPartitionReader yeltPartitionLinkedListReader0 = YeltPartitionReader.Initialise(yeltPartitioner0);
            var layerYelt1 = revoLayerDayYeltRepository.GetLayerYelt1().Result;
            YeltPartitioner yeltPartitioner1 = new YeltPartitioner(ranges, layerYelt1);
            YeltPartitionReader yeltPartitionLinkedListReader1 = YeltPartitionReader.Initialise(yeltPartitioner1);

            long[] sortedKeys = new long[yeltPartitionLinkedListReader0.TotalLength + yeltPartitionLinkedListReader1.TotalLength];
            fixed (long* keysPtr = sortedKeys)
            {
                long* ptr = keysPtr;
                int keyCount = YeltPartitionMerge.Merge_ScalarOptimised_2(yeltPartitionLinkedListReader0.Head, yeltPartitionLinkedListReader1.Head, ptr);
                Array.Resize(ref sortedKeys, keyCount);
            }
            YeltPartitionMapper mapper = new YeltPartitionMapper(new[] { yeltPartitionLinkedListReader0, yeltPartitionLinkedListReader1 }, sortedKeys);

            int j = 0;
            Stopwatch sw = Stopwatch.StartNew();
            int[] rowsFromDictionary = null;
            while (j++ <= 100)
            {
                rowsFromDictionary = mapper.MapKeysFromDictionary();
                mapper.Reset();
            }
            sw.Stop();
            System.Console.WriteLine($"{nameof(mapper.MapKeysFromDictionary)}->{sw}");

            //mapper.Reset();
            //sw.Restart();
            //j = 0;
            //int[] rowsFromFixedDirectory = null;
            //while (j++ <= 100)
            //{
            //    rowsFromFixedDirectory = mapper.MapKeysFromFixedDirectory();
            //    mapper.Reset();
            //}
            //sw.Stop();
            //System.Console.WriteLine($"{nameof(mapper.MapKeysFromFixedDirectory)}->{sw}");

            mapper.Reset();
            sw.Restart();
            j = 0;
            int[] rowsFromDynamicDirectory = null;
            while (j++ <= 100)
            {
                rowsFromDynamicDirectory = mapper.MapKeysFromDynamicDirectory();
                mapper.Reset();
            }
            sw.Stop();
            System.Console.WriteLine($"{nameof(mapper.MapKeysFromDynamicDirectory)}->{sw}");

            mapper.Reset();
            sw.Restart();
            j = 0;
            int[] rowsFromDynamicDirectory2 = null;
            while (j++ <= 100)
            {
                rowsFromDynamicDirectory2 = mapper.MapKeysFromDynamicDirectory2();
                mapper.Reset();
            }
            sw.Stop();
            System.Console.WriteLine($"{nameof(mapper.MapKeysFromDynamicDirectory2)}->{sw}");

            mapper.Reset();
            sw.Restart();
            j = 0;
            int[] rowsFromDynamicDirectory3 = null;
            while (j++ <= 100)
            {
                rowsFromDynamicDirectory3 = mapper.MapKeysFromDynamicDirectory3();
                mapper.Reset();
            }
            sw.Stop();
            System.Console.WriteLine($"{nameof(mapper.MapKeysFromDynamicDirectory3)}->{sw}");

            mapper.Reset();
            sw.Restart();
            j = 0;
            int[] rowsFromDynamicDirectory5 = null;
            while (j++ <= 100)
            {
                rowsFromDynamicDirectory5 = mapper.MapKeysFromDynamicDirectory5();
                mapper.Reset();
            }
            sw.Stop();
            System.Console.WriteLine($"{nameof(mapper.MapKeysFromDynamicDirectory5)}->{sw}");

            //if (rowsFromFixedDirectory.Length != rowsFromDictionary.Length)
            //    throw new Exception("Not Equal");
            //for (int ii = 0; ii < rowsFromFixedDirectory.Length; ii++)
            //{
            //    if (rowsFromFixedDirectory[ii] != rowsFromDictionary[ii])
            //        throw new Exception("Not Equal");
            //}

            if (rowsFromDynamicDirectory.Length != rowsFromDictionary.Length)
                throw new Exception("Not Equal");
            for (int ii = 0; ii < rowsFromDynamicDirectory.Length; ii++)
            {
                if (rowsFromDynamicDirectory[ii] != rowsFromDictionary[ii])
                    throw new Exception("Not Equal");
            }

            if (rowsFromDynamicDirectory2.Length != rowsFromDictionary.Length)
                throw new Exception("Not Equal 2");
            for (int ii = 0; ii < rowsFromDynamicDirectory2.Length; ii++)
            {
                if (rowsFromDynamicDirectory2[ii] != rowsFromDictionary[ii])
                    throw new Exception("Not Equal 2");
            }

            if (rowsFromDynamicDirectory3.Length != rowsFromDictionary.Length)
                throw new Exception("Not Equal 3");
            for (int ii = 0; ii < rowsFromDynamicDirectory3.Length; ii++)
            {
                if (rowsFromDynamicDirectory3[ii] != rowsFromDictionary[ii])
                    throw new Exception("Not Equal 3");
            }

            if (rowsFromDynamicDirectory5.Length != rowsFromDictionary.Length)
                throw new Exception("Not Equal 5");
            for (int ii = 0; ii < rowsFromDynamicDirectory5.Length; ii++)
            {
                if (rowsFromDynamicDirectory5[ii] != rowsFromDictionary[ii])
                    throw new Exception("Not Equal 5");
            }
        }
    }
}
