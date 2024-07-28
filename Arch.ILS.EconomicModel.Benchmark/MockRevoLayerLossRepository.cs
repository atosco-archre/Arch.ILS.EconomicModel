
using System.Runtime.CompilerServices;

namespace Arch.ILS.EconomicModel.Benchmark
{
    internal class MockRevoLayerLossRepository : IRevoLayerLossUploadRepository
    {
        private readonly RevoLayerYeltEntry[] _data;
        public MockRevoLayerLossRepository(int sampleSize)
        {
            _data = GetRevoLayerYeltEntries(sampleSize).ToArray();
        }

        #region Methods

        public Task<RevoLayerYeltFixedDictionary> GetLayerYeltFixedDictionary()
        {
            return Task.Factory.StartNew<RevoLayerYeltFixedDictionary>((state) =>
            {
                return new(1, 1, (RevoLayerYeltEntry[])state!);
            }, _data);
        }

        public Task<RevoLayerYeltYearArray> GetLayerYeltYearArray()
        {
            return Task.Factory.StartNew<RevoLayerYeltYearArray>((state) =>
            {
                return new(1, 1, (RevoLayerYeltEntry[])state!);
            }, _data);
        }

        public Task<RevoLayerYeltStandard> GetLayerYeltStandard()
        {
            return Task.Factory.StartNew<RevoLayerYeltStandard>((state) =>
            {
                return new(1, 1, (RevoLayerYeltEntry[])state!);
            }, _data);
        }

        public Task<RevoLayerYeltStandardUnmanaged> GetLayerYeltStandardUnmanaged()
        {
            return Task.Factory.StartNew<RevoLayerYeltStandardUnmanaged>((state) =>
            {
                return new(1, 1, (RevoLayerYeltEntry[])state!);
            }, _data);
        }

        public Task<RevoLayerYeltStandardUnsafe> GetLayerYeltStandardUnsafe()
        {
            return Task.Factory.StartNew<RevoLayerYeltStandardUnsafe>((state) =>
            {
                return new(1, 1, (RevoLayerYeltEntry[])state!);
            }, _data);
        }

        public Task<RevoLayerYeltUnmanaged> GetLayerYeltUnmanaged()
        {
            return Task.Factory.StartNew<RevoLayerYeltUnmanaged>((state) =>
            {
                return new(1, 1, (RevoLayerYeltEntry[])state!);
            }, _data);
        }

        public Task<RevoLayerYeltUnmanagedVectorised> GetLayerYeltUnmanagedVectorised()
        {
            return Task.Factory.StartNew<RevoLayerYeltUnmanagedVectorised>((state) =>
            {
                return new(1, 1, (RevoLayerYeltEntry[])state!);
            }, _data);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private IEnumerable<RevoLayerYeltEntry> GetRevoLayerYeltEntries(int sampleSize)
        {
            Random random = new Random();
            for(int i = 0; i < sampleSize; ++i)
            {
                yield return new RevoLayerYeltEntry
                {
                    Year = (short)random.Next(0, 10000),
                    EventId = random.Next(1, 1000000000),
                    Peril = "WS",
                    Day = (short)random.Next(1, 365),
                    LossPct = random.NextDouble(),
                    RP = random.NextDouble(),
                    RB = random.NextDouble(),
                };
            }
        }


        #endregion Methods
    }
}
