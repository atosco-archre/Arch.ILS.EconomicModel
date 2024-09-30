using Arch.ILS.EconomicModel.Benchmark;
using Arch.ILS.EconomicModel.Benchmark.Indexer;


/*Upload*/
//LayerUploadBenchmark layerUploadBenchmark = new LayerUploadBenchmark();
//layerUploadBenchmark.RunBenchmarks();

//MockRevoLayerYeltRepository mockRevoLayerLossRepository = new MockRevoLayerYeltRepository(1001);
//var test = mockRevoLayerLossRepository.GetLayerYeltUnmanagedVectorised().Result;
//Console.ReadLine();

/*Indexer*/
LayerKeyIndexerBenchmark layerIndexerBenchmark = new LayerKeyIndexerBenchmark();
layerIndexerBenchmark.RunBenchmarks();

//IndexerCheckTest.Execute();