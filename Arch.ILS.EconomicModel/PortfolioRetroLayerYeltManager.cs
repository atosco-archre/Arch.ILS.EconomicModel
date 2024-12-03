
using Arch.ILS.EconomicModel.Binary;

namespace Arch.ILS.EconomicModel
{
    public class PortfolioRetroLayerYeltManager : YeltManager, IYeltManager, IDisposable
    {
        public const int DEFAULT_TIMER_DUETIME_IN_MILLISECONDS = 0;
        public const int DEFAULT_TIMER_PERIOD_IN_MILLISECONDS = 60000;
        public const string ARCHIVE_SUFFIX = "_Archived.bin";

        private readonly string _yeltStorageFolderPath;
        private readonly IRevoRepository _revoRepository;
        private readonly IRevoLayerLossRepository _revoLayerLossRepository;
        private readonly IPortfolioRetroLayerRepository _portfolioRetroLayerRepository;
        private readonly HashSet<(int, int)> _selectedPortfolioRetros;
        private Dictionary<(int portfolioId, int retroId), Dictionary<int, PortfolioRetroLayer>> _portfolioRetroLayers;
        private long _currentMaxRowVersion;
        private Timer _timer;
        private bool _disposed;

        public PortfolioRetroLayerYeltManager(in ViewType viewType, in string yeltStorageFolderPath, IRevoRepository revoRepository, IRevoLayerLossRepository revoLayerLossRepository, HashSet<(int portfolioId, int retroId)> selectedPortfolioRetros = null) :
            base(revoRepository)
        {
            ViewType = viewType;
            _yeltStorageFolderPath = yeltStorageFolderPath;
            _revoRepository = revoRepository;
            _revoLayerLossRepository = revoLayerLossRepository;
            _portfolioRetroLayerRepository = revoRepository;
            _selectedPortfolioRetros = selectedPortfolioRetros;
            _disposed = false;
        }

        public ViewType ViewType { get; }

        public void Initialise(bool pauseRepoUpdate = false)
        {
            if (_selectedPortfolioRetros != null)
            {
                _portfolioRetroLayers = _portfolioRetroLayerRepository.GetPortfolioRetroLayers().Result
                    .Where(x => _selectedPortfolioRetros.Contains((x.PortfolioId, x.RetroProgramId)))
                    .GroupBy(g => (g.PortfolioId, g.RetroProgramId))
                    .ToDictionary(k => k.Key, v => v.ToDictionary(kk => kk.LayerId));
            }
            else
            {
                _portfolioRetroLayers = _portfolioRetroLayerRepository.GetPortfolioRetroLayers().Result
                    .GroupBy(g => (g.PortfolioId, g.RetroProgramId))
                    .ToDictionary(k => k.Key, v => v.ToDictionary(kk => kk.LayerId));
            }

            _currentMaxRowVersion = _portfolioRetroLayers.Values.SelectMany(x => x.Values).Select(y => y.RowVersion).Max();

#if DEBUG
            foreach(PortfolioRetroLayer portfolioRetroLayer in _portfolioRetroLayers.Values.SelectMany(x => x.Values))
#else
            Parallel.ForEach(_portfolioRetroLayers.Values.SelectMany(x => x.Values), new ParallelOptions { MaxDegreeOfParallelism = 2 }, portfolioRetroLayer =>
            {
#endif
                UpdateStorage(in portfolioRetroLayer, pauseRepoUpdate);
#if !DEBUG
            });
#endif
        }

        public void ScheduleSynchronisation(int dueTimeInMilliseconds = DEFAULT_TIMER_DUETIME_IN_MILLISECONDS, int periodInMilliseconds = DEFAULT_TIMER_PERIOD_IN_MILLISECONDS)
        {
            if (_timer != null)
            {
                _timer?.Change(Timeout.Infinite, Timeout.Infinite);
                _timer?.Dispose();
            }
            _timer = new Timer((obj) => { Synchronise(); }, null, dueTimeInMilliseconds, periodInMilliseconds);
        }

        public void CancelSchedule()
        {
            if (_timer != null)
            {
                _timer?.Change(Timeout.Infinite, Timeout.Infinite);
                _timer?.Dispose();
                _timer = null;
            }
        }

        public void Synchronise(bool pauseRepoUpdate = false)
        {
#if DEBUG
            Console.WriteLine($"Synchronise Layer YELTs based on Retros...{DateTime.Now}");
#endif
            base.Synchronise();
#if DEBUG
            foreach (var portfolioRetroLayer in _portfolioRetroLayerRepository.GetPortfolioRetroLayers(_currentMaxRowVersion).Result)
#else
            Parallel.ForEach(_portfolioRetroLayerRepository.GetPortfolioRetroLayers(_currentMaxRowVersion).Result, (portfolioRetroLayer) =>
#endif
            {
                (int PortfolioId, int RetroProgramId) key = (portfolioRetroLayer.PortfolioId, portfolioRetroLayer.RetroProgramId);
                if (_selectedPortfolioRetros != null && !_selectedPortfolioRetros.Contains(key))
#if DEBUG
                    continue;
#else
                    return;
#endif
                if (!_portfolioRetroLayers.TryGetValue(key, out var layers))
                {
                    layers = new Dictionary<int, PortfolioRetroLayer>();
                    _portfolioRetroLayers[key] = layers;
                }

                if (layers.TryGetValue(portfolioRetroLayer.LayerId, out var layer) && layer.RowVersion > portfolioRetroLayer.RowVersion)
                    throw new Exception();

                layers[portfolioRetroLayer.LayerId] = portfolioRetroLayer;
                if (portfolioRetroLayer.RowVersion > _currentMaxRowVersion)
                    _currentMaxRowVersion = portfolioRetroLayer.RowVersion;
                UpdateStorage(in portfolioRetroLayer, pauseRepoUpdate);
            }
#if !DEBUG
            );
#endif
        }

        public bool TryGetPortfolioRetroLayers(int portfolioId, int retroProgramId, out IEnumerable<PortfolioRetroLayer> portfolioRetroLayers)
        {
            return TryGetPortfolioRetroLayers((portfolioId, retroProgramId), out portfolioRetroLayers);
        }

        public bool TryGetPortfolioRetroLayers((int portfolioId, int retroProgramId) portfolioRetroId, out IEnumerable<PortfolioRetroLayer> portfolioRetroLayers)
        {
            if (_portfolioRetroLayers.TryGetValue(portfolioRetroId, out var layersById))
            {
                portfolioRetroLayers = layersById.Values;
                return true;
            }
            else
            {
                portfolioRetroLayers = Enumerable.Empty<PortfolioRetroLayer>();
                return false;
            }

        }

        public bool TryGetLatestLayerLossAnalysis(in int layerId, in RevoLossViewType revoLossViewType, out LayerLossAnalysis layerLossAnalysis)
        {
            return TryGetLatestLayerLossAnalysis(in layerId, in revoLossViewType, ViewType, out layerLossAnalysis);
        }

        private void UpdateStorage(in PortfolioRetroLayer portfolioRetroLayer, bool pauseUpdate)
        {
            if (TryGetValue(ViewType, portfolioRetroLayer.LayerId, out var lossViewLayerLossAnalyses))
            {
#if DEBUG
                foreach(var lossViewAnalyses in lossViewLayerLossAnalyses)
#else
                Parallel.ForEach(lossViewLayerLossAnalyses, (lossViewAnalyses) =>
#endif
                {
                    LayerLossAnalysis latestLossAnalysis = lossViewAnalyses.Value[0];
                    string filePath = Path.Combine(_yeltStorageFolderPath, YeltFileInfoFactory.GetFileNameWithExtension(latestLossAnalysis.LossAnalysisId, latestLossAnalysis.LayerId, latestLossAnalysis.RowVersion));
                    string fileNamePrefix = YeltFileInfoFactory.GetFileNamePrefix(latestLossAnalysis.LossAnalysisId, latestLossAnalysis.LayerId);
                    IYelt layerYelt = null;
                    bool newFileAdded = false;
                    string[] filesToArchive = null;
                    if (!pauseUpdate)
                    {
                        filesToArchive = Directory.GetFiles(_yeltStorageFolderPath, $"{fileNamePrefix}*").Where(x => !x.EndsWith(ARCHIVE_SUFFIX)).ToArray();
                        if (!Path.Exists(filePath))
                        {
                            layerYelt = _revoLayerLossRepository.GetLayerDayYeltVectorised(latestLossAnalysis.LossAnalysisId, latestLossAnalysis.LayerId).Result;
                            layerYelt.RowVersion = latestLossAnalysis.RowVersion;
                            RevoYeltBinaryWriter revoYeltBinaryWriter = new RevoYeltBinaryWriter(layerYelt);
                            revoYeltBinaryWriter.WriteAll(filePath);
                            newFileAdded = true;
                        }
                    }
                    if (!YeltStorage.ContainsKey(latestLossAnalysis.LossAnalysisId, latestLossAnalysis.LayerId, latestLossAnalysis.RowVersion))
                    {
                        if (layerYelt == null)
                        {
                            if (File.Exists(filePath))
                            {
                                RevoYeltBinaryReader revoYeltBinaryReader = new RevoYeltBinaryReader(filePath);
                                layerYelt = revoYeltBinaryReader.ReadAll();
                            }
                            else if (!pauseUpdate)
                                throw new FileNotFoundException(filePath);
                        }
                        
                        if(layerYelt != null)
                            YeltStorage.TryAdd(layerYelt);
                    }

                    if (newFileAdded)
                        Archive(filesToArchive);
                }
#if !DEBUG
                );
#endif
            }
        }

        private bool IsFileLocked(string filePath)
        {
            try
            {
                using (FileStream stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.None))
                {
                    stream.Close();
                }
            }
            catch (IOException)
            {
                //the file is unavailable because it is:
                //still being written to
                //or being processed by another thread
                //or does not exist (has already been processed)
                return true;
            }

            //file is not locked
            return false;
        }

        private void Archive(string[] filesToArchive)
        {
            foreach (string fileToArchive in filesToArchive)
            {
                if (IsFileLocked(fileToArchive))
                {
#if DEBUG
                    Console.WriteLine($"{fileToArchive} - File in use - Cannot be archived...");
#endif
                }
                else
                {
#if DEBUG
                    Console.WriteLine($"{fileToArchive} - Archiving...");
#endif
                    File.Move(fileToArchive, $"{Path.Combine(Path.GetDirectoryName(fileToArchive), Path.GetFileNameWithoutExtension(fileToArchive))}{ARCHIVE_SUFFIX}");
                }
            }
        }

        public void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
#if DEBUG
                    Console.WriteLine("Retro Layer Yelt Synchronisation Timer - Disposing...");
#endif
                    CancelSchedule();
#if DEBUG
                    Console.WriteLine("Retro Layer Yelt Synchronisation Timer - Disposed...");
#endif
                }
            }

            _disposed = true;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
