
namespace Arch.ILS.EconomicModel.Benchmark
{
    internal class MockRevoLayerDayYeltRepository
    {
        private readonly RevoLayerYeltEntry[] _data;
        public MockRevoLayerDayYeltRepository(int sampleSize)
        {
            _data = GetRevoLayerYeltEntries(sampleSize * 3).ToArray();
        }

        #region Methods

        public Task<RevoLayerDayYeltVectorised2> GetLayerYelt0()
        {
            return Task.Factory.StartNew<RevoLayerDayYeltVectorised2>((state) =>
            {
                return new(1, 1, (RevoLayerYeltEntry[])state!);
            }, _data.Where((x, i) => (i % 3) == 0 || (i % 2) == 0).ToArray());
        }

        public Task<RevoLayerDayYeltVectorised2> GetLayerYelt1()
        {
            return Task.Factory.StartNew<RevoLayerDayYeltVectorised2>((state) =>
            {
                return new(1, 1, (RevoLayerYeltEntry[])state!);
            }, _data.Where((x, i) => (i % 3) == 1 || (i % 4) == 0).ToArray());
        }

        public Task<RevoLayerDayYeltVectorised2> GetLayerYelt2()
        {
            return Task.Factory.StartNew<RevoLayerDayYeltVectorised2>((state) =>
            {
                return new(1, 1, (RevoLayerYeltEntry[])state!);
            }, _data.Where((x, i) => (i % 3) == 2 || (i % 8) == 0).ToArray());
        }

        private IEnumerable<RevoLayerYeltEntry> GetRevoLayerYeltEntries(int sampleSize)
        {
            Random random = new Random();
            for(int i = 0; i < sampleSize; ++i)
            {
                var day = (short)random.Next(1, 365);
                if (day == 50)
                    ++day;
                yield return new RevoLayerYeltEntry
                {
                    Year = (short)random.Next(0, 10000),
                    EventId = random.Next(1, 1000000000),
                    Peril = "WS",
                    Day = day,
                    LossPct = random.NextDouble(),
                    RP = random.NextDouble(),
                    RB = random.NextDouble(),
                };
            }
        }


        #endregion Methods
    }
}
