using Arch.ILS.EconomicModel.Benchmark;

//LayerUploadBenchmark layerUploadBenchmark = new LayerUploadBenchmark();
//layerUploadBenchmark.RunBenchmarks();
MockRevoLayerLossRepository mockRevoLayerLossRepository = new MockRevoLayerLossRepository(1001);
var test = mockRevoLayerLossRepository.GetLayerYeltUnmanagedVectorised().Result;
Console.ReadLine();