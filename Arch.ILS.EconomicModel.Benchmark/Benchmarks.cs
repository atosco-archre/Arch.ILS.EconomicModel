
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Running;

namespace Arch.EconomicModel.Benchmark
{
    public class Benchmarks<T> where T : class
    {
        public Benchmarks()
        {
        }

        public void Run_Benchmarks<T>()
        {
            var logger = new AccumulationLogger();

            var config = ManualConfig.Create(DefaultConfig.Instance)
                .AddLogger(logger)
                .WithOptions(ConfigOptions.DisableOptimizationsValidator);

            BenchmarkRunner.Run<T>(config);

            // write benchmark summary
            Console.WriteLine(logger.GetLog());
        }
    }
}
