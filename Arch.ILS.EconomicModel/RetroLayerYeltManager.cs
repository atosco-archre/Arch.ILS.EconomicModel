
using Arch.ILS.EconomicModel.Binary;

namespace Arch.ILS.EconomicModel
{
    public class RetroLayerYeltManager : YeltManager
    {
        private readonly string _yeltStorageFolderPath;
        private readonly IRevoRepository _revoRepository;
        private readonly IRevoLayerLossRepository _revoLayerLossRepository;
        private readonly IRetroLayerRepository _retroLayerRepository;
        private readonly HashSet<int> _selectedRetros;
        private Dictionary<int, Dictionary<int, RetroLayer>> _retroLayers;
        private long _currentMaxRowVersion;

        public RetroLayerYeltManager(string yeltStorageFolderPath, IRevoRepository revoRepository, IRevoLayerLossRepository revoLayerLossRepository, HashSet<int> selectedRetros = null) :
            base(revoRepository)
        {
            _yeltStorageFolderPath = yeltStorageFolderPath;
            _revoRepository = revoRepository;
            _revoLayerLossRepository = revoLayerLossRepository;
            _retroLayerRepository = revoRepository;
            _selectedRetros = selectedRetros;
        }

        public void Initialise()
        {
            if (_selectedRetros != null)
            {
                _retroLayers = _retroLayerRepository.GetRetroLayers().Result
                    .Where(x => _selectedRetros.Contains(x.RetroProgramId))
                    .GroupBy(g => g.RetroProgramId)
                    .ToDictionary(k => k.Key, v => v.ToDictionary(kk => kk.LayerId));
            }
            else
            {
                _retroLayers = _retroLayerRepository.GetRetroLayers().Result
                    .GroupBy(g => g.RetroProgramId)
                    .ToDictionary(k => k.Key, v => v.ToDictionary(kk => kk.LayerId));
            }

            _currentMaxRowVersion = _retroLayers.Values.SelectMany(x => x.Values).Select(y => y.RowVersion).Max();

#if DEBUG
            foreach(RetroLayer retroLayer in _retroLayers.Values.SelectMany(x => x.Values))
#else
            Parallel.ForEach(_retroLayers.Values.SelectMany(x => x.Values), new ParallelOptions { MaxDegreeOfParallelism = 2 }, retroLayer =>
            {
#endif
                UpdateStorage(in retroLayer);
#if !DEBUG
            });
#endif
        }

        public override void Increment()
        {
            base.Increment();
#if DEBUG
            foreach (var retroLayer in _retroLayerRepository.GetRetroLayers(_currentMaxRowVersion).Result)
#else
            Parallel.ForEach(_retroLayerRepository.GetRetroLayers(_currentMaxRowVersion).Result, (retroLayer) =>
#endif
            {
                if (_selectedRetros != null && !_selectedRetros.Contains(retroLayer.RetroProgramId))
#if DEBUG
                    continue;
#else
                    return;
#endif
                if (!_retroLayers.TryGetValue(retroLayer.RetroProgramId, out var layers))
                {
                    layers = new Dictionary<int, RetroLayer>();
                    _retroLayers[retroLayer.RetroProgramId] = layers;
                }

                if (layers.TryGetValue(retroLayer.LayerId, out var layer) && layer.RowVersion > retroLayer.RowVersion)
                    throw new Exception();

                layers[retroLayer.LayerId] = retroLayer;
                if (retroLayer.RowVersion > _currentMaxRowVersion)
                    _currentMaxRowVersion = retroLayer.RowVersion;
                UpdateStorage(in retroLayer);
            }
#if !DEBUG
            );
#endif
        }

        private void UpdateStorage(in RetroLayer retroLayer)
        {
            if(_lossAnalysesByLayerLossView.TryGetValue(retroLayer.LayerId, out var lossViewLayerLossAnalyses))
            {
#if DEBUG
                foreach(var lossViewAnalyses in lossViewLayerLossAnalyses)
#else
                Parallel.ForEach(lossViewLayerLossAnalyses, (lossViewAnalyses) =>
#endif
                {
                    LayerLossAnalysis latestLossAnalysis = lossViewAnalyses.Value[0];
                    string filePath = Path.Combine(_yeltStorageFolderPath, YeltFileInfoFactory.GetFileNameWithExtension(latestLossAnalysis.LossAnalysisId, latestLossAnalysis.LayerId, latestLossAnalysis.RowVersion));
                    IYelt layerYelt = null;
                    if (!Path.Exists(filePath))
                    {
                        layerYelt = _revoLayerLossRepository.GetLayerDayYeltVectorised(latestLossAnalysis.LossAnalysisId, latestLossAnalysis.LayerId).Result;
                        RevoYeltBinaryWriter revoYeltBinaryWriter = new RevoYeltBinaryWriter(layerYelt);
                        revoYeltBinaryWriter.WriteAll(filePath);
                    }

                    if(!_yeltStorage.ContainsKey(latestLossAnalysis.LossAnalysisId, latestLossAnalysis.LayerId, latestLossAnalysis.RowVersion))
                    {
                        if(layerYelt == null)
                        {
                            RevoYeltBinaryReader revoYeltBinaryReader = new RevoYeltBinaryReader(filePath);
                            layerYelt = revoYeltBinaryReader.ReadAll();
                        }
                        _yeltStorage.TryAdd(layerYelt);
                    }
                }
#if !DEBUG
                );
#endif
            }
        }
    }
}
