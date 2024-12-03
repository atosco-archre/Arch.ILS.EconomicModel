
namespace Arch.ILS.EconomicModel
{
    public class YeltManager
    {
        private readonly ILayerLossAnalysisRepository _layerLossAnalysisRepository;
        private Dictionary<int, Dictionary<RevoLossViewType, List<LayerLossAnalysis>>> _lossAnalysesByLayerLossView;
        private Dictionary<int, Dictionary<RevoLossViewType, List<LayerLossAnalysis>>> _lossAnalysesByLayerLossViewRemapped;

        private long _currentMaxRowVersion;

        public YeltManager(ILayerLossAnalysisRepository layerLossAnalysisRepository)
        {
            _layerLossAnalysisRepository = layerLossAnalysisRepository;
            YeltStorage = YeltStorage.CreateOrGetInstance();
            Initialise();
        }

        public YeltStorage YeltStorage { get; }

        private void Initialise()
        {
            _lossAnalysesByLayerLossView = _layerLossAnalysisRepository.GetLayerLossAnalyses().Result
                .GroupBy(g => g.LayerId)
                .ToDictionary(k => k.Key, v => v
                    .GroupBy(gg => gg.LossView)
                    .ToDictionary(kk => kk.Key, vv => vv.OrderByDescending(o => o.RowVersion).ToList()));

            _currentMaxRowVersion = _lossAnalysesByLayerLossView.Values.SelectMany(x => x.Values.First().Select(y => y.RowVersion)).Max();
            UpdateRemap();
        }

        public virtual void Synchronise()
        {
            foreach (var layerLossAnalysis in _layerLossAnalysisRepository.GetLayerLossAnalyses(_currentMaxRowVersion).Result)
            {
                if (!_lossAnalysesByLayerLossView.TryGetValue(layerLossAnalysis.LayerId, out var layersLossAnalyses))
                {
                    layersLossAnalyses = new Dictionary<RevoLossViewType, List<LayerLossAnalysis>>();
                    _lossAnalysesByLayerLossView[layerLossAnalysis.LayerId] = layersLossAnalyses;
                }

                if (!layersLossAnalyses.TryGetValue(layerLossAnalysis.LossView, out var layerLossAnalyses))
                {
                    layerLossAnalyses = new List<LayerLossAnalysis>();
                    layersLossAnalyses[layerLossAnalysis.LossView] = layerLossAnalyses;
                }

                if (layerLossAnalyses.Count == 0)
                    throw new Exception($"No loss analysis found for LayerId {layerLossAnalysis.LayerId} - LossAnalysisId {layerLossAnalysis.LossAnalysisId} - LossView {layerLossAnalysis.LossView.ToString()}");

                if (layerLossAnalysis.RowVersion < layerLossAnalyses[0].RowVersion)
                    throw new Exception();

                layerLossAnalyses.Insert(0, layerLossAnalysis);
                if (layerLossAnalysis.RowVersion > _currentMaxRowVersion)
                    _currentMaxRowVersion = layerLossAnalysis.RowVersion;
            }

            UpdateRemap();
        }

        public bool TryGetLatestLayerLossAnalysis(in int layerId, in RevoLossViewType revoLossViewType, in ViewType viewType, out LayerLossAnalysis layerLossAnalysis)
        {
            if (GetLossAnalysesByLayerView(viewType).TryGetValue(layerId, out var lossViewTypesAnalyses)
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

        protected bool TryGetValue(in ViewType viewType, in int layerId, out Dictionary<RevoLossViewType, List<LayerLossAnalysis>> lossViewLayerLossAnalyses)
        {
            var source = viewType == ViewType.InForce ? _lossAnalysesByLayerLossView : _lossAnalysesByLayerLossViewRemapped;
            return source.TryGetValue(layerId, out lossViewLayerLossAnalyses);
        }

        private static RevoLossViewType Remap(RevoLossViewType revoLossViewType)
        {
            return revoLossViewType switch
            {
                RevoLossViewType.ArchRevised => RevoLossViewType.ArchView,
                RevoLossViewType.StressedRevised => RevoLossViewType.StressedView,
                RevoLossViewType.BudgetArchView => RevoLossViewType.ArchView,
                RevoLossViewType.BudgetStressedView => RevoLossViewType.StressedView,
                RevoLossViewType.BudgetClientView => RevoLossViewType.ClientView,
                _ => revoLossViewType
            };
        }

        private void UpdateRemap()
        {
            _lossAnalysesByLayerLossViewRemapped = _lossAnalysesByLayerLossView
                .ToDictionary(k => k.Key, v => v.Value
                    .GroupBy(gg => Remap(gg.Key), gv => gv.Value)
                    .ToDictionary(kk => kk.Key, vv => vv.SelectMany(s => s).OrderByDescending(o => o.RowVersion).ToList()));
        }

        private Dictionary<int, Dictionary<RevoLossViewType, List<LayerLossAnalysis>>> GetLossAnalysesByLayerView(ViewType viewType)
        {
            return viewType switch
            {
                ViewType.InForce => _lossAnalysesByLayerLossView,
                ViewType.Projected => _lossAnalysesByLayerLossViewRemapped,
                _ => throw new NotImplementedException($"ViewType {viewType}")
            };
        }
    }
}
