
namespace Arch.ILS.EconomicModel
{
    public class YeltManager
    {
        #region Types

        private record class LossAnalysisPriority(LayerLossAnalysis LayerLossAnalysis, in int Priority);

        private class LossAnalysisPriorityComparer : IComparer<LossAnalysisPriority>
        {
            public int Compare(LossAnalysisPriority x, LossAnalysisPriority y)
            {
                int compare = x.Priority.CompareTo(y.Priority);
                return (compare == 0)
                    ? y.LayerLossAnalysis.RowVersion.CompareTo(x.LayerLossAnalysis.RowVersion)
                    : compare;

            }
        }

        #endregion Types

        #region Constants

        private const int LOSS_VIEW_PRIORITY_COUNT = 3;//private const int LOSS_VIEW_PRIORITY_COUNT = 4;
        private const int LOSS_VIEW_PRIORITY_INDEX_0 = 0;
        private const int LOSS_VIEW_PRIORITY_INDEX_1 = 1;
        private const int LOSS_VIEW_PRIORITY_INDEX_2 = 2;
        //private const int LOSS_VIEW_PRIORITY_INDEX_3 = 3;

        #endregion Constants

        #region Variables

        private static LossAnalysisPriorityComparer _lossAnalysisPriorityComparer;
        private readonly ILayerRepository _layerRepository;
        private readonly ILayerLossAnalysisRepository _layerLossAnalysisRepository;
        private readonly HashSet<SegmentType> _segmentFilter;
        private Dictionary<int, Dictionary<RevoLossViewType, List<LayerLossAnalysis>>> _lossAnalysesByLayerLossView;
        private Dictionary<int, Dictionary<RevoLossViewType, List<LayerLossAnalysis>>> _lossAnalysesByLayerLossViewRemapped;
        private long _currentMaxRowVersion;

        #endregion Variables

        #region Constructors

        static YeltManager()
        {
            _lossAnalysisPriorityComparer = new LossAnalysisPriorityComparer();
        }

        public YeltManager(ILayerRepository layerRepository, ILayerLossAnalysisRepository layerLossAnalysisRepository, HashSet<SegmentType> segmentFilter)
        {
            _layerRepository = layerRepository;
            _layerLossAnalysisRepository = layerLossAnalysisRepository;
            _segmentFilter = segmentFilter;
            YeltStorage = YeltStorage.CreateOrGetInstance();
        }

        #endregion Constructors

        #region Properties

        public YeltStorage YeltStorage { get; }

        #endregion Properties

        #region Methods

        protected Task Initialise()
        {
            return Task.Factory.StartNew(() =>
            {
                var layerSegment = _layerRepository.GetLayerMetaInfos().Result;
                _lossAnalysesByLayerLossView = _layerLossAnalysisRepository.GetLayerLossAnalyses().Result
                    .Where(w => _segmentFilter == null || (layerSegment.ContainsKey(w.LayerId) && _segmentFilter.Contains(layerSegment[w.LayerId].Segment)))
                    .GroupBy(g => g.LayerId)
                    .ToDictionary(k => k.Key, v => v
                        .GroupBy(gg => gg.LossView)
                        .ToDictionary(kk => kk.Key, vv => vv.OrderByDescending(o => o.RowVersion).ToList()));

                _currentMaxRowVersion = _lossAnalysesByLayerLossView.Values.SelectMany(x => x.Values.First().Select(y => y.RowVersion)).Max();
                UpdateRemap();
            });
        }

        public Task Synchronise()
        {
            return Task.Factory.StartNew(() =>
            {
                var layerSegment = _layerRepository.GetLayerMetaInfos().Result;
                foreach (var layerLossAnalysis in _layerLossAnalysisRepository.GetLayerLossAnalyses(_currentMaxRowVersion).Result)
                {
                    if (_segmentFilter != null && !(layerSegment.ContainsKey(layerLossAnalysis.LayerId) && _segmentFilter.Contains(layerSegment[layerLossAnalysis.LayerId].Segment)))
                        continue;

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
            });
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
                    .SelectMany(s => s.Value)
                    .GroupBy(g => Remap(g.LossView))
                    .ToDictionary(kk => kk.Key, vv => vv
                        .Select(ss => new LossAnalysisPriority( ss, AssignPriorityOnRemap(ss.LossView)))
                        .OrderBy(o => o, _lossAnalysisPriorityComparer)
                        .Select(x => x.LayerLossAnalysis).ToList()));
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

        private static int AssignPriorityOnRemap(RevoLossViewType lossViewType)
        {
            return lossViewType switch
            {
                RevoLossViewType.ArchRevised => LOSS_VIEW_PRIORITY_INDEX_0,
                RevoLossViewType.StressedRevised => LOSS_VIEW_PRIORITY_INDEX_0,  

                RevoLossViewType.ArchView => LOSS_VIEW_PRIORITY_INDEX_1,
                RevoLossViewType.StressedView => LOSS_VIEW_PRIORITY_INDEX_1,
                RevoLossViewType.ClientView => LOSS_VIEW_PRIORITY_INDEX_1,

                /*We need to first review the budget view before using it*/
                //RevoLossViewType.BudgetArchView => LOSS_VIEW_PRIORITY_INDEX_2,
                //RevoLossViewType.BudgetStressedView => LOSS_VIEW_PRIORITY_INDEX_2,
                //RevoLossViewType.BudgetClientView => LOSS_VIEW_PRIORITY_INDEX_2,
                //_ => LOSS_VIEW_PRIORITY_INDEX_3
                _ => LOSS_VIEW_PRIORITY_INDEX_2
            };
        }

        #endregion Methods

    }
}
