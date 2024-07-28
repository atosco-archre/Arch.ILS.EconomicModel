
using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Arch.Common;
using Arch.EconomicModel;

ConnectionProtection connectionProtection =
    new ConnectionProtection(@"C:\Users\atosco\source\repos\Arch.EconomicModel\Arch.EconomicModel.Console\App.config.config");
//if (!connectionProtection.IsProtected())
//    connectionProtection.EncryptFile();
//RevoSnowflakeRepository revoSnowflakeRepository = new RevoSnowflakeRepository(new SnowflakeConnectionStrings().ConnectionString);
//var layers = revoSnowflakeRepository.GetLayers().Result;
RevoConnectionStrings connectionSettings = new RevoConnectionStrings(connectionProtection, false);
RevoSqlRepository revoRepository = new RevoSqlRepository(connectionSettings.GetConnectionString(RevoConnectionStrings.REVO));
//var Layers = revoRepository.GetLayers().Result.ToDictionary(x => x.LayerId);
//var portfolios = revoRepository.GetPortfolios().Result.ToDictionary(x => x.PortfolioId);
//var portLayers = revoRepository.GetPortfolioLayers().Result.ToDictionary(x => x.PortLayerId);

int partitionCount = 8;
Thread.Sleep(5000);
Stopwatch sw = Stopwatch.StartNew();
//revoRepository.GetPortfolioLayerCessions().Wait();
//sw.Stop();
//Console.WriteLine(sw.Elapsed);
//Thread.Sleep(5000);
//sw.Restart();

//revoRepository.GetPortfolioLayerCessionsFast(partitionCount).Wait();
//sw.Stop();
//Console.WriteLine(sw.Elapsed);
//Thread.Sleep(5000);
//sw.Restart();

//revoRepository.GetPortfolioLayerCessionsFast2(partitionCount).Wait();
//sw.Stop();
//Console.WriteLine(sw.Elapsed);
//Thread.Sleep(5000);
//sw.Restart();

//revoRepository.GetPortfolioLayerCessionsFast3(partitionCount).Wait();
//sw.Stop();
//Console.WriteLine(sw.Elapsed);
//Thread.Sleep(5000);
//sw.Restart();

//revoRepository.GetPortfolioLayerCessionsFast4(partitionCount).Wait();
//sw.Stop();
//Console.WriteLine(sw.Elapsed);
//Thread.Sleep(5000);
//sw.Restart();

//revoRepository.GetPortfolioLayerCessionsFast6(partitionCount);
//sw.Stop();
//Console.WriteLine(sw.Elapsed);
////Thread.Sleep(5000);
//sw.Restart();

//revoRepository.GetPortfolioLayerCessionsNoStorage();
//sw.Stop();
//Console.WriteLine(sw.Elapsed);
////Thread.Sleep(5000);
//sw.Restart();

//revoRepository.GetPortfolioLayerCessionsNoStorageParallel();
//sw.Stop();
//Console.WriteLine(sw.Elapsed);
////Thread.Sleep(5000);
//sw.Restart();

//revoRepository.GetPortfolioLayerCessionsNoStorageTasks();
//sw.Stop();
//Console.WriteLine(sw.Elapsed);
////Thread.Sleep(5000);
//sw.Restart();

revoRepository.GetLayerView();
sw.Stop();
Console.WriteLine(sw.Elapsed);
//Thread.Sleep(5000);
sw.Restart();

Console.WriteLine("End.");

Console.WriteLine("End.");