
namespace Arch.ILS.EconomicModel
{
    public class YeltManager
    {
        private readonly ILayerLossAnalysisRepository _layerLossAnalysisRepository;
        
        protected Dictionary<int, Dictionary<RevoLossViewType, List<LayerLossAnalysis>>> _lossAnalysesByLayerLossView;
        private long _currentMaxRowVersion;

        public YeltManager(ILayerLossAnalysisRepository layerLossAnalysisRepository)
        {
            _layerLossAnalysisRepository = layerLossAnalysisRepository;
            YeltStorage = YeltStorage.CreateOrGetInstance();
            Initialise();
        }

        public YeltStorage YeltStorage{ get; }

        private void Initialise()
        {
            _lossAnalysesByLayerLossView = _layerLossAnalysisRepository.GetLayerLossAnalyses().Result
                .GroupBy(g => g.LayerId)
                .ToDictionary(k => k.Key, v => v
                    .GroupBy(gg => gg.LossView)
                    .ToDictionary(kk => kk.Key, vv => vv.OrderByDescending(o => o.RowVersion).ToList()));

            _currentMaxRowVersion = _lossAnalysesByLayerLossView.Values.SelectMany(x => x.Values.First().Select(y => y.RowVersion)).Max();
        }

        public virtual void Synchronise()
        {
            foreach(var layerLossAnalysis in _layerLossAnalysisRepository.GetLayerLossAnalyses(_currentMaxRowVersion).Result)
            {
                if(!_lossAnalysesByLayerLossView.TryGetValue(layerLossAnalysis.LayerId, out var layersLossAnalyses))
                {
                    layersLossAnalyses = new Dictionary<RevoLossViewType, List<LayerLossAnalysis>>();
                    _lossAnalysesByLayerLossView[layerLossAnalysis.LayerId] = layersLossAnalyses;
                }

                if(!layersLossAnalyses.TryGetValue(layerLossAnalysis.LossView, out var layerLossAnalyses))
                {
                    layerLossAnalyses = new List<LayerLossAnalysis>();
                    layersLossAnalyses[layerLossAnalysis.LossView] = layerLossAnalyses;
                }

                if(layerLossAnalyses.Count == 0)
                    throw new Exception($"No loss analysis found for LayerId {layerLossAnalysis.LayerId} - LossAnalysisId {layerLossAnalysis.LossAnalysisId} - LossView {layerLossAnalysis.LossView.ToString()}");

                if (layerLossAnalysis.RowVersion < layerLossAnalyses[0].RowVersion)
                    throw new Exception();

                layerLossAnalyses.Insert(0, layerLossAnalysis);
                if(layerLossAnalysis.RowVersion > _currentMaxRowVersion)
                    _currentMaxRowVersion = layerLossAnalysis.RowVersion;
            }
        }

        public bool TryGetLatestLayerLossAnalysis(int layerId, RevoLossViewType revoLossViewType, out LayerLossAnalysis layerLossAnalysis)
        {
            if(_lossAnalysesByLayerLossView.TryGetValue(layerId, out var lossViewTypesAnalyses)
                && lossViewTypesAnalyses.TryGetValue(revoLossViewType, out var lossAnalyses)
                && lossAnalyses.Count > 0)
            {
                layerLossAnalysis = lossAnalyses[0];
                return true;
            }
            else
            {
                layerLossAnalysis = null;
                return false;
            }
        }
    }
}
