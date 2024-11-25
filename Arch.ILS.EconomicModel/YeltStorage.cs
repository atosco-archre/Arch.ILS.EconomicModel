
using System.Collections.Concurrent;

namespace Arch.ILS.EconomicModel
{
    public class YeltStorage
    {
        private static YeltStorage _yeltStorage;
        private readonly ConcurrentDictionary<(int lossAnalysisId, int layerId, long rowVersion), IYelt> _yelts;

        private YeltStorage() 
        {
            _yelts = new ConcurrentDictionary<(int lossAnalysisId, int layerId, long rowVersion), IYelt>();
        }

        public static YeltStorage CreateOrGetInstance()
        {
            if(_yeltStorage == null) 
                _yeltStorage = new YeltStorage();
            return _yeltStorage;
        }

        public bool ContainsKey(in int lossAnalysisId, in int layerId, in long rowVersion)
        {
            return _yelts.ContainsKey((lossAnalysisId, layerId, rowVersion));
        }

        public bool TryGetValue(in int lossAnalysisId, in int layerId, in long rowVersion, out IYelt yelt)
        {
            return _yelts.TryGetValue((lossAnalysisId, layerId, rowVersion), out yelt);
        }

        public bool TryAdd(IYelt yelt)
        {
            return _yelts.TryAdd((yelt.LossAnalysisId, yelt.LayerId, yelt.RowVersion), yelt);
        }

        public bool Remove(IYelt yelt)
        {
            return _yelts.Remove((yelt.LossAnalysisId, yelt.LayerId, yelt.RowVersion), out IYelt storedYelt);
        }
    }
}
