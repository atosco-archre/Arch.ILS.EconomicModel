
using Arch.ILS.EconomicModel.Binary;

namespace Arch.ILS.EconomicModel
{
    public class RetroLayerYeltManager : YeltManager, IYeltManager, IDisposable
    {
        public const int DEFAULT_TIMER_DUETIME_IN_MILLISECONDS = 0;
        public const int DEFAULT_TIMER_PERIOD_IN_MILLISECONDS = 60000;
        public const string ARCHIVE_SUFFIX = "_Archived.bin";

        private readonly string _yeltStorageFolderPath;
        private readonly IRevoRepository _revoRepository;
        private readonly IRevoLayerLossRepository _revoLayerLossRepository;
        private readonly IRetroLayerRepository _retroLayerRepository;
        private readonly HashSet<int> _selectedRetros;
        private Dictionary<int, Dictionary<int, RetroLayer>> _retroLayers;
        private long _currentMaxRowVersion;
        private Timer _timer;
        private bool _disposed;

        public RetroLayerYeltManager(in ViewType viewType, string yeltStorageFolderPath, IRevoRepository revoRepository, IRevoLayerLossRepository revoLayerLossRepository, HashSet<int> selectedRetros = null, HashSet<SegmentType> segmentFilter = null) 
            : base(revoRepository, revoRepository, segmentFilter)
        {
            ViewType = viewType;
            _yeltStorageFolderPath = yeltStorageFolderPath;
            _revoRepository = revoRepository;
            _revoLayerLossRepository = revoLayerLossRepository;
            _retroLayerRepository = revoRepository;
            _selectedRetros = selectedRetros;
            _disposed = false;
        }

        public ViewType ViewType { get; }

        public Task Initialise(bool pauseUpdate = false)
        {
            return Task.Factory.StartNew(() =>
            {
                base.Initialise().Wait();
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
                    UpdateStorage(in retroLayer, pauseUpdate);
#if !DEBUG
                });
#endif
            });
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

        public Task Synchronise(bool pauseUpdate = false)
        {
            return Task.Factory.StartNew(() =>
            {
#if DEBUG
                Console.WriteLine($"Synchronise Layer YELTs based on Retros...{DateTime.Now}");
#endif
                base.Synchronise().Wait();
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
                    UpdateStorage(in retroLayer, pauseUpdate);
                }
#if !DEBUG
            );
#endif
            });
        }

        public bool TryGetRetroLayers(int retroProgramId, out IEnumerable<RetroLayer> retroLayers)
        {
            if (_retroLayers.TryGetValue(retroProgramId, out var layersById))
            {
                retroLayers = layersById.Values;
                return true;
            }
            else
            {
                retroLayers = Enumerable.Empty<RetroLayer>();
                return false;
            }

        }

        public bool TryGetLatestLayerLossAnalysis(in int layerId, in RevoLossViewType revoLossViewType, out LayerLossAnalysis layerLossAnalysis)
        {
            return TryGetLatestLayerLossAnalysis(in layerId, in revoLossViewType, ViewType, out layerLossAnalysis);
        }

        private void UpdateStorage(in RetroLayer retroLayer, bool pauseUpdate)
        {
            if (TryGetValue(ViewType, retroLayer.LayerId, out var lossViewLayerLossAnalyses))
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
