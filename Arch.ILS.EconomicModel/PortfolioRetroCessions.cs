
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Arch.ILS.EconomicModel
{
    public class PortfolioRetroCessions
    {
        /*Cessions By PortFolioId - RetroLevelType - RetroProgramId - PortLayerId*/
        //private readonly Dictionary<int, Dictionary<byte, Dictionary<int, ReadOnlyDictionary<int, PortLayerCessionExtended>>>> _portfolioLevelRetroCessions;

        /*Cessions By PortFolioId - RetroLevelType - PortLayerId - RetroProgramId*/
        private readonly Dictionary<int, Dictionary<byte, Dictionary<int, ReadOnlyDictionary<int, PortLayerCessionExtended>>>> _portfolioLevelLayerRetroCessions;
        private readonly Dictionary<int, Dictionary<byte, List<PortLayerPeriodCession>>> _portfolioLevelLayerPeriodRetroCessions;

        public PortfolioRetroCessions(IEnumerable<PortLayerCessionExtended> portLayerCessions) 
        {
            //_portfolioLevelRetroCessions = portLayerCessions
            //    .GroupBy(portCessions => portCessions.PortfolioId)
            //    .ToDictionary(portCessionPort => portCessionPort.Key, portCessionPortGroup => portCessionPortGroup
            //        .GroupBy(portGroup => portGroup.RetroLevelType)
            //        .ToDictionary(portRetroLevel => portRetroLevel.Key, portRetroLevelGroup => portRetroLevelGroup
            //            .GroupBy(portRetroLevelProgram => portRetroLevelProgram.RetroProgramId)
            //            .ToDictionary(retroCessions => retroCessions.Key, retroCessionsValue => new ReadOnlyDictionary<int, PortLayerCessionExtended>(retroCessionsValue
            //                .ToDictionary(retroCessions => retroCessions.PortLayerId)))));
            _portfolioLevelLayerRetroCessions = portLayerCessions
                .GroupBy(portCessions => portCessions.PortfolioId)
                .ToDictionary(portCessionPort => portCessionPort.Key, portCessionPortGroup => portCessionPortGroup
                    .GroupBy(portGroup => portGroup.RetroLevelType)
                    .ToDictionary(portRetroLevel => portRetroLevel.Key, portRetroLevelGroup => portRetroLevelGroup
                        .GroupBy(portLayerCession => portLayerCession.PortLayerId)
                        .ToDictionary(layerCession => layerCession.Key, layerCessionValue => new ReadOnlyDictionary<int, PortLayerCessionExtended>(layerCessionValue
                            .ToDictionary(retroCessions => retroCessions.RetroProgramId)))));
            _portfolioLevelLayerPeriodRetroCessions = new Dictionary<int, Dictionary<byte, List<PortLayerPeriodCession>>>();
            SetNetCessions();
        }

        public IEnumerable<int> GetPortfolioIds()
        {
            return _portfolioLevelLayerRetroCessions.Keys;
        }

        public IEnumerable<byte> GetPortfolioLevels(in int portfolioId)
        {
            return _portfolioLevelLayerRetroCessions[portfolioId].Keys;
        }

        public IEnumerable<int> GetPortfolioLevelLayers(in int portfolioId, in byte retroLevelId)
        {
            return _portfolioLevelLayerRetroCessions[portfolioId][retroLevelId].Keys;
        }

        public IEnumerable<int> GetPortfolioLevelLayerRetroPrograms(in int portfolioId, in byte retroLevelId, in int portLayerId)
        {
            return _portfolioLevelLayerRetroCessions[portfolioId][retroLevelId][portLayerId].Keys;
        }

        public IEnumerable<PortLayerCessionExtended> GetPortfolioLevelLayerRetroCessions(in int portfolioId, in byte retroLevelId, in int portLayerId)
        {
            return _portfolioLevelLayerRetroCessions[portfolioId][retroLevelId][portLayerId].Values;
        }

        public IEnumerable<PortLayerPeriodCession> GetPortfolioLevelLayerCessions()
        {
            return _portfolioLevelLayerPeriodRetroCessions.Values.SelectMany(a => a.Values).SelectMany(b => b);
        }

        private void SetNetCessions()
        {
            Stopwatch sw = Stopwatch.StartNew();
            sw.Start();
#if DEBUG
             Parallel.ForEach(_portfolioLevelLayerRetroCessions.Keys, new ParallelOptions { MaxDegreeOfParallelism = 1 }, (portfolioId) => { SetNetCessions(portfolioId);});
#else
            Parallel.ForEach(_portfolioLevelLayerRetroCessions.Keys, (portfolioId) => { SetNetCessions(portfolioId); });
#endif
            sw.Stop();
            Console.WriteLine($"Net Cession elapsed {sw.Elapsed}.");
        }

        private unsafe void SetNetCessions(in int portfolioId)
        {
            var levels = GetPortfolioLevels(portfolioId).OrderBy(x => x).ToArray();
            Span<byte> sLevels = new Span<byte>(levels);
            Dictionary<int, List<PeriodCession>> cumulativeCessions = new Dictionary<int, List<PeriodCession>>();
            Dictionary<byte, List<PortLayerPeriodCession>> levelPortLayerPeriodCessions = new Dictionary<byte, List<PortLayerPeriodCession>>();
            for(int l = 0; l < sLevels.Length; ++l)
            {
                byte level = sLevels[l];
                var layerRetroCessions = _portfolioLevelLayerRetroCessions[portfolioId][level];
                List<PortLayerPeriodCession> currentLevelPeriodCessions = new List<PortLayerPeriodCession>();
                foreach (var layerCessions in layerRetroCessions)
                {
                    int portLayerId = layerCessions.Key;                 
                    List<PortLayerPeriodCession> currentLevelLayerPeriodCessions = new List<PortLayerPeriodCession>();

                    if (!cumulativeCessions.TryGetValue(portLayerId, out List<PeriodCession> layerCumulativeCessions) || layerCumulativeCessions.Count == 0)
                        layerCumulativeCessions = null;
                        
                    foreach (PortLayerCessionExtended retroCession in layerCessions.Value.Values)
                    {
                        if(layerCumulativeCessions != null)
                        {
                            var periodCession = new PeriodCession(retroCession.OverlapStart, retroCession.OverlapEnd, retroCession.CessionGross);
                            var sLayerCumulativeCessions = CollectionsMarshal.AsSpan<PeriodCession>(layerCumulativeCessions);
                            FindNetCessions(new PortLayerPeriodCession(retroCession.RetroLevelType, retroCession.RetroProgramId, in portLayerId, ref periodCession), sLayerCumulativeCessions, ref currentLevelLayerPeriodCessions);
                        }
                        else
                        {
                            PeriodCession periodCession = new PeriodCession(retroCession.OverlapStart, retroCession.OverlapEnd, retroCession.CessionGross);
                            PortLayerPeriodCession cession = new PortLayerPeriodCession(retroCession.RetroLevelType, retroCession.RetroProgramId, in portLayerId, ref periodCession);
                            currentLevelLayerPeriodCessions.Add(cession);
                        }
                    }
                    if (cumulativeCessions.TryGetValue(portLayerId, out var layerPreviousLevelsCessions))
                        layerPreviousLevelsCessions.AddRange(currentLevelLayerPeriodCessions.Select(x => x.PeriodCession));
                    else
                        layerPreviousLevelsCessions = currentLevelLayerPeriodCessions.Select(x => x.PeriodCession).ToList();
                    cumulativeCessions[portLayerId] = FindInuringCessions(layerPreviousLevelsCessions);

                    currentLevelPeriodCessions.AddRange(currentLevelLayerPeriodCessions);
                }
                levelPortLayerPeriodCessions.Add(level, currentLevelPeriodCessions);
            }

            _portfolioLevelLayerPeriodRetroCessions[portfolioId] = levelPortLayerPeriodCessions;
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

        private static void FindNetCessions(PortLayerPeriodCession currentLayerRetroCession, Span<PeriodCession> previousCumulativeCessions, ref List<PortLayerPeriodCession> output)
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
                FindNetCessions(new PortLayerPeriodCession(currentLayerRetroCession.RetroLevel, currentLayerRetroCession.RetroProgramId, currentLayerRetroCession.PortLayerId, ref cession), previousCumulativeCessions, ref output);
            }

            if (rangeA.TryGetRightNonOverlap(ref rangeB, out DateTimeRange rightNonOverlap))
            {
                PeriodCession cession = new PeriodCession(rightNonOverlap.StartInclusive, rightNonOverlap.EndInclusive, currentLayerRetroCession.PeriodCession.NetCession);
                FindNetCessions(new PortLayerPeriodCession(currentLayerRetroCession.RetroLevel, currentLayerRetroCession.RetroProgramId, currentLayerRetroCession.PortLayerId, ref cession), previousCumulativeCessions, ref output);
            }

            if (rangeA.TryGetOverlap(ref rangeB, out DateTimeRange overlap))
            {
                PeriodCession cession = new PeriodCession(overlap.StartInclusive, overlap.EndInclusive, currentLayerRetroCession.PeriodCession.NetCession * (1.0m - cessionB.NetCession));
                FindNetCessions(new PortLayerPeriodCession(currentLayerRetroCession.RetroLevel, currentLayerRetroCession.RetroProgramId, currentLayerRetroCession.PortLayerId, ref cession), previousCumulativeCessions, ref output);
            }
        }
    }
}
