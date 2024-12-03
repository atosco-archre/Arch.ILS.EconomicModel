
using System.Collections.ObjectModel;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Arch.ILS.EconomicModel
{
    public class RetroCessions
    {
        /*Cessions By RetroLevelType - LayerId - RetroProgramId*/
        private readonly Dictionary<byte, Dictionary<int, ReadOnlyDictionary<(int RetroProgramId, int RetroProgramResetId), RetroLayerCession>>> _levelLayerRetroCessions;
        private Dictionary<byte, List<LayerPeriodCession>> _levelLayerPeriodRetroCessions;

        public RetroCessions(IEnumerable<RetroLayerCession> portLayerCessions) 
        {

            _levelLayerRetroCessions = portLayerCessions
                .GroupBy(portGroup => portGroup.RetroLevelType)
                .ToDictionary(portRetroLevel => portRetroLevel.Key, portRetroLevelGroup => portRetroLevelGroup
                    .GroupBy(portLayerCession => portLayerCession.LayerId)
                    .ToDictionary(layerCession => layerCession.Key, layerCessionValue => new ReadOnlyDictionary<(int RetroProgramId, int RetroProgramResetId), RetroLayerCession>(layerCessionValue
                        .ToDictionary(retroCessions => (retroCessions.RetroProgramId, retroCessions.RetroProgramResetId)))));
            SetNetCessions();
        }

        public IEnumerable<byte> GetLevels()
        {
            return _levelLayerRetroCessions.Keys;
        }

        public IEnumerable<int> GetLevelLayers(in byte retroLevelId)
        {
            return _levelLayerRetroCessions[retroLevelId].Keys;
        }

        public IEnumerable<int> GetLevelLayerRetroPrograms(in byte retroLevelId, in int layerId)
        {
            return _levelLayerRetroCessions[retroLevelId][layerId].Keys.Select(x => x.RetroProgramId).Distinct();
        }

        public IEnumerable<RetroLayerCession> GetLevelLayerRetroCessions(in byte retroLevelId, in int layerId)
        {
            return _levelLayerRetroCessions[retroLevelId][layerId].Values;
        }

        public IEnumerable<LayerPeriodCession> GetLevelLayerCessions()
        {
            return _levelLayerPeriodRetroCessions.SelectMany(a => a.Value);
        }

        private unsafe void SetNetCessions()
        {
            var levels = GetLevels().OrderBy(x => x).ToArray();
            Span<byte> sLevels = new Span<byte>(levels);
            Dictionary<int, List<PeriodCession>> cumulativeCessions = new Dictionary<int, List<PeriodCession>>();
            Dictionary<byte, List<LayerPeriodCession>> levelLayerPeriodCessions = new Dictionary<byte, List<LayerPeriodCession>>();
            for(int l = 0; l < sLevels.Length; ++l)
            {
                byte level = sLevels[l];
                var layerRetroCessions = _levelLayerRetroCessions[level];
                List<LayerPeriodCession> currentLevelPeriodCessions = new List<LayerPeriodCession>();
                foreach (var layerCessions in layerRetroCessions)
                {
                    int layerId = layerCessions.Key;                 
                    List<LayerPeriodCession> currentLevelLayerPeriodCessions = new List<LayerPeriodCession>();

                    if (!cumulativeCessions.TryGetValue(layerId, out List<PeriodCession> layerCumulativeCessions) || layerCumulativeCessions.Count == 0)
                        layerCumulativeCessions = null;
                        
                    foreach (RetroLayerCession retroCession in layerCessions.Value.Values)
                    {
                        if(layerCumulativeCessions != null)
                        {
                            var periodCession = new PeriodCession(retroCession.OverlapStart, retroCession.OverlapEnd, retroCession.CessionGross);
                            var sLayerCumulativeCessions = CollectionsMarshal.AsSpan<PeriodCession>(layerCumulativeCessions);
                            FindNetCessions(new LayerPeriodCession(retroCession.RetroLevelType, retroCession.RetroProgramId, in layerId, retroCession.CessionGross, ref periodCession), sLayerCumulativeCessions, ref currentLevelLayerPeriodCessions);
                        }
                        else
                        {
                            PeriodCession periodCession = new PeriodCession(retroCession.OverlapStart, retroCession.OverlapEnd, retroCession.CessionGross);
                            LayerPeriodCession cession = new LayerPeriodCession(retroCession.RetroLevelType, retroCession.RetroProgramId, in layerId, retroCession.CessionGross, ref periodCession);
                            currentLevelLayerPeriodCessions.Add(cession);
                        }
                    }
                    if (cumulativeCessions.TryGetValue(layerId, out var layerPreviousLevelsCessions))
                        layerPreviousLevelsCessions.AddRange(currentLevelLayerPeriodCessions.Select(x => x.PeriodCession));
                    else
                        layerPreviousLevelsCessions = currentLevelLayerPeriodCessions.Select(x => x.PeriodCession).ToList();
                    cumulativeCessions[layerId] = FindInuringCessions(layerPreviousLevelsCessions);

                    currentLevelPeriodCessions.AddRange(currentLevelLayerPeriodCessions);
                }
                levelLayerPeriodCessions.Add(level, currentLevelPeriodCessions);
            }

            _levelLayerPeriodRetroCessions = levelLayerPeriodCessions;
        }

        private static List<PeriodCession> FindInuringCessions(List<PeriodCession> previousCessions)
        {
            DateTime[] startDates = previousCessions.Select(x => x.StartInclusive).Distinct().OrderBy(d => d).ToArray();
            DateTime[] endDates = previousCessions.Select(x => x.EndInclusive).Distinct().OrderBy(d => d).ToArray();
            Span<DateTime> sStarts = startDates;
            Span<DateTime> sEnds = endDates;
            Span<PeriodCession> sPreviousCessions = CollectionsMarshal.AsSpan(previousCessions);
            List<PeriodCession> periodCessions = new List<PeriodCession>();
            ref DateTime nextStartDate = ref MemoryMarshal.GetReference(sStarts);
            ref DateTime afterLastStartDate = ref Unsafe.Add(ref nextStartDate, sStarts.Length);
            ref DateTime nextEndDate = ref MemoryMarshal.GetReference(sEnds);
            ref DateTime afterLastEndDate = ref Unsafe.Add(ref nextEndDate, sEnds.Length);
            DateTime cursor = nextStartDate;
            nextStartDate = ref Unsafe.Add(ref nextStartDate, 1);

            while (Unsafe.IsAddressLessThan(ref nextStartDate, ref afterLastStartDate))
            {
                if (nextStartDate <= nextEndDate)
                {
                    DateTimeRange range = new DateTimeRange(cursor, nextStartDate.AddDays(-1));
                    if(DateTimeRange.IsValid(ref range))
                        SetPeriodCessions(ref range, ref sPreviousCessions, ref periodCessions);
                    cursor = nextStartDate;
                    nextStartDate = ref Unsafe.Add(ref nextStartDate, 1);
                }
                else
                {
                    DateTimeRange range = new DateTimeRange(cursor, nextEndDate);
                    if (DateTimeRange.IsValid(ref range))
                        SetPeriodCessions(ref range, ref sPreviousCessions, ref periodCessions);
                    cursor = nextEndDate.AddDays(1);
                    nextEndDate = ref Unsafe.Add(ref nextEndDate, 1);
                }
            }

            DateTimeRange lastFromStartrange = new DateTimeRange(cursor, nextEndDate);
            if (DateTimeRange.IsValid(ref lastFromStartrange))
                SetPeriodCessions(ref lastFromStartrange, ref sPreviousCessions, ref periodCessions);
            cursor = nextEndDate.AddDays(1);
            nextEndDate = ref Unsafe.Add(ref nextEndDate, 1);

            while (Unsafe.IsAddressLessThan(ref nextEndDate, ref afterLastEndDate))
            {
                DateTimeRange fromEndRange = new DateTimeRange(cursor, nextEndDate);
                if (DateTimeRange.IsValid(ref fromEndRange))
                    SetPeriodCessions(ref fromEndRange, ref sPreviousCessions, ref periodCessions);

                cursor = nextEndDate.AddDays(1);
                nextEndDate = ref Unsafe.Add(ref nextEndDate, 1);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static void SetPeriodCessions(ref DateTimeRange range, ref Span<PeriodCession> sPreviousCessions, ref List<PeriodCession> periodCessions)
            {
                decimal cumulativeCession = decimal.Zero;
                for (int j = 0; j < sPreviousCessions.Length; j++)
                {
                    var cessionRange = new DateTimeRange(sPreviousCessions[j].StartInclusive, sPreviousCessions[j].EndInclusive);
                    if (range.TryGetOverlap(ref cessionRange, out DateTimeRange overlap))
                        cumulativeCession += sPreviousCessions[j].NetCession;
                }

                if (cumulativeCession != decimal.Zero)
                    periodCessions.Add(new PeriodCession(range.StartInclusive, range.EndInclusive, cumulativeCession));
            }
            return periodCessions;
        }

        private static void FindNetCessions(LayerPeriodCession currentLayerRetroCession, Span<PeriodCession> previousCumulativeCessions, ref List<LayerPeriodCession> output)
        {
            if (previousCumulativeCessions.Length == 0)
            {
                output.Add(currentLayerRetroCession);
                return;
            }

            ref PeriodCession cessionB = ref previousCumulativeCessions[0];
            previousCumulativeCessions = previousCumulativeCessions[1..];
            DateTimeRange rangeA = new DateTimeRange(currentLayerRetroCession.PeriodCession.StartInclusive, currentLayerRetroCession.PeriodCession.EndInclusive);
            DateTimeRange rangeB = new DateTimeRange(cessionB.StartInclusive, cessionB.EndInclusive);
            if (rangeA.TryGetLeftNonOverlap(ref rangeB, out DateTimeRange leftNonOverlap))
            {
                PeriodCession cession = new PeriodCession(leftNonOverlap.StartInclusive, leftNonOverlap.EndInclusive, currentLayerRetroCession.PeriodCession.NetCession);
                FindNetCessions(new LayerPeriodCession(currentLayerRetroCession.RetroLevel, currentLayerRetroCession.RetroProgramId, currentLayerRetroCession.LayerId, currentLayerRetroCession.GrossCession, ref cession), previousCumulativeCessions, ref output);
            }

            if (rangeA.TryGetRightNonOverlap(ref rangeB, out DateTimeRange rightNonOverlap))
            {
                PeriodCession cession = new PeriodCession(rightNonOverlap.StartInclusive, rightNonOverlap.EndInclusive, currentLayerRetroCession.PeriodCession.NetCession);
                FindNetCessions(new LayerPeriodCession(currentLayerRetroCession.RetroLevel, currentLayerRetroCession.RetroProgramId, currentLayerRetroCession.LayerId, currentLayerRetroCession.GrossCession, ref cession), previousCumulativeCessions, ref output);
            }

            if (rangeA.TryGetOverlap(ref rangeB, out DateTimeRange overlap))
            {
                PeriodCession cession = new PeriodCession(overlap.StartInclusive, overlap.EndInclusive, currentLayerRetroCession.PeriodCession.NetCession * (1.0m - cessionB.NetCession));
                FindNetCessions(new LayerPeriodCession(currentLayerRetroCession.RetroLevel, currentLayerRetroCession.RetroProgramId, currentLayerRetroCession.LayerId, currentLayerRetroCession.GrossCession, ref cession), previousCumulativeCessions, ref output);
            }
        }
    }
}
