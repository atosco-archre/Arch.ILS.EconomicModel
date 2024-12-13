﻿
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Amazon.S3.Model;
using Arch.ILS.Common;
using Arch.ILS.Core;
using Arch.ILS.EconomicModel;
using Arch.ILS.EconomicModel.Binary;
using Arch.ILS.Snowflake;
using Google.Apis.Storage.v1.Data;
using static Arch.ILS.EconomicModel.Console.Program;

namespace Arch.ILS.EconomicModel.Console
{
    public class Program
    {
        public static unsafe void Main(string[] args)
        {
            //ExportRetroLayerCessions(@"C:\Data\RetroAllocations LOD Resets.csv", ResetType.LOD);
            //ExportRetroCessionMetrics(@"C:\Data\RetroMetrics_All.csv", new DateTime(2024, 9, 30), ResetType.RAD, true, retroIdFilter: /*new HashSet<int> { 274 }*/null);
            //ExportPremiumByRetroProfile(@"C:\Data\RetroProfilePremiums_BoundFx.csv", new DateTime(2024, 9, 30), true);
            //ExportPremiumByRetroProfile(@"C:\Data\RetroProfilePremiums_20240930Fx.csv", new DateTime(2024, 9, 30), false);
            //SetPortfolioLayerCession();

            //ProcessLayerYelts();
            ProcessRetroLayerYelts(new HashSet<int> { 317/*274*/ }, RevoLossViewType.StressedView, ViewType.Projected);
            //ProcessPortfolioRetroLayerYelts(new HashSet<(int portfolioId, int retroId)> { (1003, 274)/*, (1003, 317)*//*, (839,  247)*/ }, RevoLossViewType.StressedView, ViewType.InForce);
            //UploadRetroYelts(new HashSet<int> { 274 });
        }

        public static void ExportRetroLayerCessions(string outputFilePath, ResetType resetType)
        {
            /*Authentication*/
            RevoSnowflakeRepository revoRepository = GetRevoSnowflakeRepository();
            //var retroAllocations = revoRepository.GetRetroAllocations().Result;
            //var spInsurers = revoRepository.GetSPInsurers().Result;
            //var retroprograms = revoRepository.GetRetroPrograms().Result;
            //var retroInvestorResets = revoRepository.GetRetroInvestorResets().Result.ToList();
            //var retroProgramResets = revoRepository.GetRetroProgramResets().Result.ToList();

            //var retroResetCessions = revoRepository.GetRetroResetCessions().Result.ToList();
            //var retroInitialCessions = revoRepository.GetRetroInitialCessions().Result.ToList();
            //var investorResetCessions = revoRepository.GetInvestorResetCessions().Result.ToList();
            //var investorInitialCessions = revoRepository.GetInvestorInitialCessions().Result.ToList();
            RevoRepositoryTracker revoRepositoryTracker = new RevoRepositoryTracker(revoRepository);
            revoRepositoryTracker.Initialise([ResetType.RAD]);
            //revoRepositoryTracker.ScheduleSynchronisation();
            var retroAllocationView = revoRepositoryTracker.LatestRetroCessions[ResetType.RAD];
            var levelLayerCessions = retroAllocationView.GetLevelLayerCessions();
            var layerDetails = revoRepository.GetLayerDetails().Result;
            var retroPrograms = revoRepository.GetRetroPrograms().Result;

            using (FileStream fs = new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
            using (StreamWriter sw = new StreamWriter(fs))
            {
                sw.WriteLine($"RetroLevel,RetroProgramId,RetroProgramType,RetroInception,RetroExpiration,LayerId,LayerInception,LayerExpiration,LayerStatus,StartInclusive,StartDayOfYear,EndInclusive,EndDayOfYear,NetCession");
                foreach (var c in levelLayerCessions)
                {
                    LayerDetail layerDetail = layerDetails[c.LayerId];
                    RetroProgram retro = retroPrograms[c.RetroProgramId];
                    sw.WriteLine($"{c.RetroLevel},{c.RetroProgramId},{retro.RetroProgramType.ToString()},{retro.Inception},{retro.Expiration},{c.LayerId},{layerDetail.Inception},{layerDetail.Expiration},{layerDetail.Status.ToString()},{c.PeriodCession.StartInclusive},{c.PeriodCession.StartInclusive.DayOfYear},{c.PeriodCession.EndInclusive},{c.PeriodCession.EndInclusive.DayOfYear},{c.PeriodCession.NetCession}");
                }
                sw.Flush();
            }
        }

        public static void ExportPremiumByRetroProfile(string outputFilePath, DateTime currentFxDate, bool useBoundFx = true, int previousYear = 2023, int currentYear = 2024, string baseCurrency = "USD")
        {
            /*Authentication*/
            RevoRepository revoRepository = GetRevoSnowflakeRepository();
            var retroAllocationView = revoRepository.GetRetroCessionView();
            var retroProfiles = revoRepository.GetRetroProfiles();
            var retroPrograms = revoRepository.GetRetroPrograms();
            var layerDetails = revoRepository.GetLayerDetails();
            var submissions = revoRepository.GetSubmissions();
            var fxRates = revoRepository.GetFXRates();

            Task.WaitAll(retroAllocationView, retroProfiles, retroPrograms, layerDetails, submissions, fxRates);
            Dictionary<int, Dictionary<int, decimal>> retroProfileUwYearPremium = new Dictionary<int, Dictionary<int, decimal>>();
            var levelLayerCessions = retroAllocationView.Result.GetLevelLayerCessions();

            foreach (var layerCession in levelLayerCessions)
            {
                if (!layerDetails.Result.TryGetValue(layerCession.LayerId, out var layerDetail))
                    continue;
                /*if (layerDetail.Status != ContractStatus.Bound
                    && layerDetail.Status != ContractStatus.Signed
                    && layerDetail.Status != ContractStatus.SignReady
                    // && layerDetail.Status != ContractStatus.Budget
                    )
                    continue;*/
                if (!submissions.Result.TryGetValue(layerDetail.SubmissionId, out var submission))
                    continue;
                var fxRate = RevoHelper.GetFxRate(useBoundFx, currentFxDate, baseCurrency, submission, layerDetail, fxRates.Result);
                var retroProgram = retroPrograms.Result[layerCession.RetroProgramId];
                if (!retroProfiles.Result.TryGetValue(retroProgram.RetroProfileId, out var retroProfile))
                    continue;

                if (!retroProfileUwYearPremium.TryGetValue(retroProfile.RetroProfileId, out var uwYearsPremiums))
                {
                    uwYearsPremiums = new Dictionary<int, decimal>();
                    retroProfileUwYearPremium[retroProfile.RetroProfileId] = uwYearsPremiums;
                }

                decimal cededPremium = layerDetail.Premium
                    * (layerDetail.Placement == decimal.Zero ? decimal.Zero : 1 / layerDetail.Placement)
                    * (layerDetail.SignedShare > decimal.Zero ? layerDetail.SignedShare : layerDetail.EstimatedShare)
                    * fxRate
                    * layerCession.PeriodCession.NetCession
                    * (submission.TranType == TranType.Ceded ? -1 : (submission.TranType == TranType.Assumed ? 1 : 0))
                    * (decimal)(((layerCession.PeriodCession.EndInclusive - layerCession.PeriodCession.StartInclusive).TotalDays + 1) / ((layerDetail.Expiration - layerDetail.Inception).TotalDays + 1));
                if (uwYearsPremiums.ContainsKey(layerDetail.UWYear))
                    uwYearsPremiums[layerDetail.UWYear] += cededPremium;
                else
                    uwYearsPremiums[layerDetail.UWYear] = cededPremium;
            }

            using (FileStream fs = new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
            using (StreamWriter sw = new StreamWriter(fs))
            {
                sw.WriteLine($"RetroProfileId,RetroProfileName,LossWeight2024,ExpectedWP2023,ExpectedWP2024");
                foreach (var profileYearPremiums in retroProfileUwYearPremium.OrderBy(x => x.Key))
                {
                    int retroProfileId = profileYearPremiums.Key;
                    profileYearPremiums.Value.TryGetValue(previousYear, out var previousPremium);
                    profileYearPremiums.Value.TryGetValue(currentYear, out var currentPremium);
                    string previous = previousPremium == decimal.Zero ? string.Empty : previousPremium.ToString();
                    string current = currentPremium == decimal.Zero ? string.Empty : currentPremium.ToString();
                    sw.WriteLine($"{retroProfileId},{retroProfiles.Result[retroProfileId].Name},,{previous},{current}");
                }
                sw.Flush();
            }
        }

        public static void ExportRetroCessionMetrics(string outputFilePath, DateTime currentFxDate, ResetType resetType, bool useBoundFx = true, Currency baseCurrency = Currency.USD, HashSet<int> retroIdFilter = null)
        {
            /*Authentication*/
            IRevoRepository revoRepository = GetRevoSnowflakeRepository();
            RetroMetricsFactory retroMetricsFactory = new RetroMetricsFactory(revoRepository);
            RetroSummaryMetrics retroSummaryMetrics = retroMetricsFactory.GetRetroMetrics(currentFxDate, resetType, useBoundFx, baseCurrency, retroIdFilter).Result;

            /*Retro Metrics*/
            using (FileStream fs = new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
            using (StreamWriter sw = new StreamWriter(fs))
            {
                sw.WriteLine($"RetroProgramId,SubjectPremium,SubjectPremiumPlaced,CededPremium,SubjectLimit,SubjectLimitPlaced,CededLimit");
                foreach (KeyValuePair<int, RetroMetrics> retroMetrics in retroSummaryMetrics.RetroMetricsByRetroProgramId.OrderBy(x => x.Key))
                {
                    var limitMetrics = retroMetrics.Value.DateLimits.Values;
                    sw.WriteLine($"{retroMetrics.Key},{retroMetrics.Value.SubjectPremium},{retroMetrics.Value.SubjectPremiumPlaced},{retroMetrics.Value.CededPremium},{limitMetrics.Select(x => x.SubjectLimit).Max()},{limitMetrics.Select(x => x.SubjectLimitPlaced).Max()},{limitMetrics.Select(x => x.CededLimit).Max()}");
                }
                sw.Flush();
            }

            /*Retro Layer Metrics*/
            using (FileStream fs = new FileStream(Path.Combine(Path.GetDirectoryName(outputFilePath), $"{Path.GetFileNameWithoutExtension(outputFilePath)}_Layers.csv"), FileMode.Create, FileAccess.Write, FileShare.Read))
            using (StreamWriter sw = new StreamWriter(fs))
            {
                sw.WriteLine($"RetroLevel,RetroProgramId,RetroProgramType,RetroInceptionDate,RetroExpirationDate,LayerId,LayerInceptionDate,LayerExpirationDate,LayerStatus,CessionStartDateInclusive,CessionEndDateInclusive,SubjectPremium,SubjectPremiumPlaced,CededPremium,SubjectLimit,SubjectLimitPlaced,CededLimit,GrossCessionAfterPlacement,NetCession");
                foreach (RetroLayerMetrics m in retroSummaryMetrics.RetroLayerMetrics)
                {
                    sw.WriteLine($"{m.RetroLevel},{m.RetroProgramid},{m.RetroProgramType.ToString()},{m.RetroInceptionDate},{m.RetroExpirationDate},{m.LayerId},{m.LayerInceptionDate},{m.LayerExpirationDate},{m.LayerStatus.ToString()},{m.StartDateInclusive},{m.EndDateInclusive},{m.SubjectPremium},{m.SubjectPremiumPlaced},{m.CededPremium},{m.SubjectLimit},{m.SubjectLimitPlaced},{m.CededLimit},{m.GrossCessionAfterPlacement},{m.NetCession}");
                }
                sw.Flush();
            }
        }

        public static void ExportPortfolioLayerCession(string outputFilePath, ResetType resetType = ResetType.LOD)
        {
            /*Authentication*/
            RevoRepository revoRepository = GetRevoSnowflakeRepository();
            /*Queries*/
            var Layers = revoRepository.GetLayers().Result;
            var portfolios = revoRepository.GetPortfolios().Result;
            var portLayers = revoRepository.GetPortfolioLayers().Result;
            var portfolioRetroCessionView = revoRepository.GetPortfolioRetroCessionView(resetType).Result;
            var portfolioLevelLayerCessions = portfolioRetroCessionView.GetPortfolioLevelLayerCessions().ToArray();

            using (FileStream fs = new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
            using (StreamWriter sw = new StreamWriter(fs))
            {
                sw.WriteLine($"RetroLevel,RetroProgramId,RetroProgramType,RetroInceptionDate,RetroExpirationDate,LayerId,LayerInceptionDate,LayerExpirationDate,CessionStartDateInclusive,CessionEndDateInclusive,CededPremium,CededLimit");
                //foreach (RetroLayerMetrics retroLayerMetric in retroLayerMetrics)
                //{
                //    var retro = retroPrograms.Result[retroLayerMetric.RetroProgramid];
                //    var layer = layerDetails.Result[retroLayerMetric.LayerId];
                //    sw.WriteLine($"{retro.RetroLevelType},{retroLayerMetric.RetroProgramid},{retro.RetroProgramType.ToString()},{retro.Inception},{retro.Expiration},{retroLayerMetric.LayerId},{layer.Inception},{layer.Expiration},{layer.Status.ToString()},{retroLayerMetric.StartDateInclusive},{retroLayerMetric.EndDateInclusive},{retroLayerMetric.CededPremium},{retroLayerMetric.CededLimit}");
                //}
                sw.Flush();
            }
        }

        public static void UploadRetroYelts(HashSet<int> retroProgramIds)
        {
            IRevoRepository revoRepository = GetRevoSnowflakeRepository();
            IRevoLayerLossRepository revoLayerLossRepository = GetRevoLayerLossSnowflakeRepository();
            RetroLayerYeltManager retroLayerYeltManager = new RetroLayerYeltManager(ViewType.InForce, @"C:\Data\Revo_Yelts", revoRepository, revoLayerLossRepository, retroProgramIds);
            retroLayerYeltManager.Initialise();
            retroLayerYeltManager.ScheduleSynchronisation();
            Thread.Sleep(140000);
            retroLayerYeltManager.Dispose();
            System.Console.ReadLine();
        }

        public static void ProcessRetroLayerYelts(HashSet<int> retroProgramIds, RevoLossViewType revoLossViewType, ViewType viewType)
        {
            Stopwatch stopwatch = new Stopwatch();
            System.Console.WriteLine("Process layer Yelts - Initialisation...");
            stopwatch.Restart();
            IRevoRepository revoRepository = GetRevoSnowflakeRepository();
            IRevoLayerLossRepository revoLayerLossRepository = GetRevoLayerLossSnowflakeRepository();
            RetroLayerYeltManager retroLayerYeltManager = new RetroLayerYeltManager(viewType, @"C:\Data\Revo_Yelts", revoRepository, revoLayerLossRepository, retroProgramIds);
            retroLayerYeltManager.Initialise(true).Wait();
            stopwatch.Stop();
            System.Console.WriteLine($"Process layer Yelts - Initialisation - Time Elapsed: {stopwatch.Elapsed}...");

            System.Console.Write("Fetch Layer Yelts...");
            stopwatch.Restart();
            Dictionary<int, Dictionary<int, RetroLayer>> retroLayersByRetroIdByLayerId = new();
            foreach (int retroProgramId in retroProgramIds)
            {
                if (retroLayerYeltManager.TryGetRetroLayers(retroProgramId, out var retroLayers))
                    retroLayersByRetroIdByLayerId[retroProgramId] = retroLayers.ToDictionary(x => x.LayerId);
            }

            var firstRetroLayers = retroLayersByRetroIdByLayerId.First().Value;
            YeltStorage yeltStorage = retroLayerYeltManager.YeltStorage;
            Dictionary<int, IYelt> layerYelt = new Dictionary<int, IYelt>();
            foreach (int layerId in firstRetroLayers.Keys)
            {
                if (!retroLayerYeltManager.TryGetLatestLayerLossAnalysis(layerId, revoLossViewType, out LayerLossAnalysis layerLossAnalysis))
                    continue;
                if (yeltStorage.TryGetValue(layerLossAnalysis.LossAnalysisId, layerId, layerLossAnalysis.RowVersion, out IYelt yelt))
                    layerYelt[layerId] = yelt;
            }
            stopwatch.Stop();
            System.Console.WriteLine($"Time Elapsed: {stopwatch.Elapsed}...");

            System.Console.Write("Partition Layer Yelts ...");
            stopwatch.Restart();
            Dictionary<IYelt, YeltPartitionReader> yeltReaders = new Dictionary<IYelt, YeltPartitionReader>();
            foreach (var yelt in layerYelt.Values)
            {
                if (yelt.TotalEntryCount == 0)
                    continue;
                YeltPartitioner yeltPartitioner = new YeltPartitioner(new Range[] { new Range(1, 366) }, yelt);
                YeltPartitionReader yeltPartitionLinkedListReader = YeltPartitionReader.Initialise(yeltPartitioner);
                int totalLength = yeltPartitionLinkedListReader.TotalLength;
                if (totalLength != yelt.TotalEntryCount)
                    throw new Exception();
                yeltReaders.Add(yelt, yeltPartitionLinkedListReader);
            }

            stopwatch.Stop();
            System.Console.WriteLine($"Time Elapsed: {stopwatch.Elapsed}...");

            System.Console.Write("Get Yelt Keys ...");
            stopwatch.Restart();
            long[] mergedSortedKeys = YeltPartitionMerge.Merge_Native(yeltReaders.Values);
            //long[] mergedSortedKeys2 = YeltPartitionMerge.Merge(yeltReaders.Values);

            //if (mergedSortedKeys2.Length != mergedSortedKeys.Length)
            //    throw new Exception();

            //for (int i = 0; i < mergedSortedKeys2.Length; i++)
            //{
            //    if (mergedSortedKeys2[i] != mergedSortedKeys[i])
            //        throw new Exception();
            //}

            stopwatch.Stop();
            System.Console.WriteLine($"Time Elapsed: {stopwatch.Elapsed}...");

            System.Console.Write("Get Mapper ...");
            stopwatch.Restart();
            YeltPartitionMapper mapper = new YeltPartitionMapper(yeltReaders.Values, mergedSortedKeys);
            //int[] rows = mapper.MapKeys().MappedIndices;
            mapper.Reset();
            stopwatch.Stop();
            System.Console.WriteLine($"Time Elapsed: {stopwatch.Elapsed}...");

            //System.Console.Write("Get Basic Mapper ...");
            //stopwatch.Restart();
            //mapper.MapPartitionedKeysBasic();
            //mapper.Reset();
            //stopwatch.Stop();
            //System.Console.WriteLine($"Time Elapsed: {stopwatch.Elapsed}...");

            System.Console.Write("Get Cession Event Losses...");
            stopwatch.Restart();
            double[] eventLosses = mapper.Process(1.0);
            mapper.Reset();
            stopwatch.Stop();
            System.Console.WriteLine($"Time Elapsed: {stopwatch.Elapsed}...");

            System.Console.Write("Get Cession Event Losses B...");
            stopwatch.Restart();
            double[] eventLossesB = mapper.ProcessPartitions(1.0);
            mapper.Reset();
            stopwatch.Stop();
            System.Console.WriteLine($"Time Elapsed: {stopwatch.Elapsed}...");

            System.Console.Write("Get Cession Event Losses C...");
            stopwatch.Restart();
            double[] eventLossesC = mapper.ProcessNative(1.0);
            mapper.Reset();
            stopwatch.Stop();
            System.Console.WriteLine($"Time Elapsed: {stopwatch.Elapsed}...");

            System.Console.Write("Get Cession Event Losses D...");
            stopwatch.Restart();
            double[] eventLossesD = mapper.ProcessPartitionsNative(1.0);
            mapper.Reset();
            stopwatch.Stop();

            System.Console.WriteLine($"Time Elapsed: {stopwatch.Elapsed}...");
            System.Console.WriteLine(eventLosses.Sum());
            System.Console.WriteLine(eventLossesB.Sum());
            System.Console.WriteLine(eventLossesC.Sum());
            System.Console.WriteLine(eventLossesD.Sum());

            if (Math.Abs(eventLosses.Sum() - eventLossesB.Sum()) > 0.0000001)
                throw new Exception();

            if (Math.Abs(eventLossesB.Sum() - eventLossesC.Sum()) > 0.0000001)
                throw new Exception();

            if (Math.Abs(eventLossesB.Sum() - eventLossesD.Sum()) > 0.0000001)
                throw new Exception();
        }

        public static void ProcessPortfolioRetroLayerYelts(HashSet<(int portfolioId, int retroId)> portfolioRetroProgramIds, RevoLossViewType revoLossViewType, ViewType viewType)
        {
            Stopwatch stopwatch = new Stopwatch();
            System.Console.WriteLine("Process layer Yelts - Initialisation...");
            stopwatch.Restart();
            IRevoRepository revoRepository = GetRevoSnowflakeRepository();
            IRevoLayerLossRepository revoLayerLossRepository = GetRevoLayerLossSnowflakeRepository();
            PortfolioRetroLayerYeltManager portfolioRetroLayerYeltManager = new PortfolioRetroLayerYeltManager(viewType, @"C:\Data\Revo_Yelts", revoRepository, revoLayerLossRepository, portfolioRetroProgramIds/*, new HashSet<SegmentType> { SegmentType.PC }*/);
            portfolioRetroLayerYeltManager.Initialise(true).Wait();
            stopwatch.Stop();
            System.Console.WriteLine($"Process layer Yelts - Initialisation - Time Elapsed: {stopwatch.Elapsed}...");

            System.Console.Write("Fetch Layer Yelts...");
            stopwatch.Restart();
            Dictionary<(int portfolioId, int retroId), Dictionary<int, PortfolioRetroLayer>> portfolioRetroLayersByRetroIdByLayerId = new();
            foreach ((int portfolioId, int retroProgramId) portfolioRetroId in portfolioRetroProgramIds)
            {
                if (portfolioRetroLayerYeltManager.TryGetPortfolioRetroLayers(portfolioRetroId, out var portfolioRetroLayers))
                    portfolioRetroLayersByRetroIdByLayerId[portfolioRetroId] = portfolioRetroLayers.ToDictionary(x => x.LayerId);
            }

            var firstRetroLayers = portfolioRetroLayersByRetroIdByLayerId.First().Value;
            YeltStorage yeltStorage = portfolioRetroLayerYeltManager.YeltStorage;
            Dictionary<int, IYelt> layerYelt = new Dictionary<int, IYelt>();
            foreach (int layerId in firstRetroLayers.Keys)
            {
                if (!portfolioRetroLayerYeltManager.TryGetLatestLayerLossAnalysis(layerId, revoLossViewType, out LayerLossAnalysis layerLossAnalysis))
                    continue;
                if (yeltStorage.TryGetValue(layerLossAnalysis.LossAnalysisId, layerId, layerLossAnalysis.RowVersion, out IYelt yelt))
                    layerYelt[layerId] = yelt;
            }
            stopwatch.Stop();
            System.Console.WriteLine($"Time Elapsed: {stopwatch.Elapsed}...");

            System.Console.Write("Partition Layer Yelts ...");
            stopwatch.Restart();
            Dictionary<IYelt, YeltPartitionReader> yeltReaders = new Dictionary<IYelt, YeltPartitionReader>();
            foreach (var yelt in layerYelt.Values)
            {
                if (yelt.TotalEntryCount == 0)
                    continue;
                YeltPartitioner yeltPartitioner = new YeltPartitioner(new Range[] { new Range(1, 366) }, yelt);
                YeltPartitionReader yeltPartitionLinkedListReader = YeltPartitionReader.Initialise(yeltPartitioner);
                int totalLength = yeltPartitionLinkedListReader.TotalLength;
                if (totalLength != yelt.TotalEntryCount)
                    throw new Exception();
                yeltReaders.Add(yelt, yeltPartitionLinkedListReader);
            }
            System.Console.WriteLine($"Processing {yeltReaders.Count} Yelt Partitions... - {yeltReaders.Values.Sum(x => x.TotalLength)} entries");
            stopwatch.Stop();
            System.Console.WriteLine($"Time Elapsed: {stopwatch.Elapsed}...");

            System.Console.Write("Get Yelt Keys ...");
            stopwatch.Restart();
            long[] mergedSortedKeys = YeltPartitionMerge.Merge_Native(yeltReaders.Values);
            //long[] mergedSortedKeys2 = YeltPartitionMerge.Merge(yeltReaders.Values);

            //if (mergedSortedKeys2.Length != mergedSortedKeys.Length)
            //    throw new Exception();

            //for (int i = 0; i < mergedSortedKeys2.Length; i++)
            //{
            //    if (mergedSortedKeys2[i] != mergedSortedKeys[i])
            //        throw new Exception();
            //}

            stopwatch.Stop();
            System.Console.WriteLine($"Time Elapsed: {stopwatch.Elapsed}...");

            System.Console.Write("Get Mapper ...");
            stopwatch.Restart();
            YeltPartitionMapper mapper = new YeltPartitionMapper(yeltReaders.Values, mergedSortedKeys);
            //int[] rows = mapper.MapKeys().MappedIndices;
            mapper.Reset();
            stopwatch.Stop();
            System.Console.WriteLine($"Time Elapsed: {stopwatch.Elapsed}...");

            //System.Console.Write("Get Basic Mapper ...");
            //stopwatch.Restart();
            //mapper.MapPartitionedKeysBasic();
            //mapper.Reset();
            //stopwatch.Stop();
            //System.Console.WriteLine($"Time Elapsed: {stopwatch.Elapsed}...");

            System.Console.Write("Get Cession Event Losses...");
            stopwatch.Restart();
            double[] eventLosses = mapper.Process(1.0);
            mapper.Reset();
            stopwatch.Stop();
            System.Console.WriteLine($"Time Elapsed: {stopwatch.Elapsed}...");

            System.Console.Write("Get Cession Event Losses B...");
            stopwatch.Restart();
            double[] eventLossesB = mapper.ProcessPartitions(1.0);
            mapper.Reset();
            stopwatch.Stop();
            System.Console.WriteLine($"Time Elapsed: {stopwatch.Elapsed}...");

            System.Console.Write("Get Cession Event Losses C...");
            stopwatch.Restart();
            double[] eventLossesC = mapper.ProcessNative(1.0);
            mapper.Reset();
            stopwatch.Stop();
            System.Console.WriteLine($"Time Elapsed: {stopwatch.Elapsed}...");

            System.Console.Write("Get Cession Event Losses D...");
            stopwatch.Restart();
            double[] eventLossesD = mapper.ProcessPartitionsNative(1.0);
            mapper.Reset();
            stopwatch.Stop();

            System.Console.WriteLine($"Time Elapsed: {stopwatch.Elapsed}...");
            System.Console.WriteLine(eventLosses.Sum());
            System.Console.WriteLine(eventLossesB.Sum());
            System.Console.WriteLine(eventLossesC.Sum());
            System.Console.WriteLine(eventLossesD.Sum());

            if (Math.Abs(eventLosses.Sum() - eventLossesB.Sum()) > 0.0000001)
                throw new Exception();

            if (Math.Abs(eventLossesB.Sum() - eventLossesC.Sum()) > 0.0000001)
                throw new Exception();

            if (Math.Abs(eventLossesB.Sum() - eventLossesD.Sum()) > 0.0000001)
                throw new Exception();
        }

        public unsafe static void ProcessLayerYelts()
        {
            int bucket = 0;
            nuint i = (uint)bucket - 1;
            /*Authentication*/
            RevoRepository revoRepository = GetRevoSnowflakeRepository();
            RevoLayerLossRepository revoLayerLossRepository = GetRevoLayerLossSnowflakeRepository();
            //var layerYelt = revoLayerLossRepository.GetLayerDayYeltVectorised(10619, 38252).Result;
            //YeltPartitioner yeltPartitioner = new YeltPartitioner(new Range[] { new Range(2, 50) }, layerYelt);
            Stopwatch sx2 = Stopwatch.StartNew();
            var layerYelt = revoLayerLossRepository.GetLayerDayYeltVectorised(37477, 104181).Result;
            string filePath = @$"C:\Data\Yelt_{layerYelt.LossAnalysisId}_{layerYelt.LayerId}.bin";
            sx2.Stop();
            System.Console.WriteLine(sx2.Elapsed);
            sx2.Restart();
            RevoYeltBinaryWriter revoYeltBinaryWriter = new RevoYeltBinaryWriter(layerYelt);
            revoYeltBinaryWriter.WriteAll(filePath);
            sx2.Stop();
            System.Console.WriteLine(sx2.Elapsed);
            sx2.Restart();
            RevoYeltBinaryReader revoYeltBinaryReader = new RevoYeltBinaryReader(filePath);
            var altLayerYelt = revoYeltBinaryReader.ReadAll();
            sx2.Stop();
            System.Console.WriteLine(sx2.Elapsed);
            sx2.Restart();
            YeltPartitioner yeltPartitioner = new YeltPartitioner(new Range[] { new Range(1, 365) }, altLayerYelt);
            YeltPartitionReader yeltPartitionLinkedListReader = YeltPartitionReader.Initialise(yeltPartitioner);
            var layerYelt2 = revoLayerLossRepository.GetLayerDayYeltVectorised(48551, 140198).Result;
            YeltPartitioner yeltPartitioner2 = new YeltPartitioner(new Range[] { new Range(2, 50) }, layerYelt2);
            YeltPartitionReader yeltPartitionLinkedListReader2 = YeltPartitionReader.Initialise(yeltPartitioner2);

            long[] sortedKeys = new long[yeltPartitionLinkedListReader.TotalLength + yeltPartitionLinkedListReader2.TotalLength];
            fixed (long* keysPtr = sortedKeys)
            {
                long* ptr = keysPtr;
                int keyCount = YeltPartitionMerge.Merge_ScalarOptimised_2(yeltPartitionLinkedListReader.Head, yeltPartitionLinkedListReader2.Head, ptr);
                Array.Resize(ref sortedKeys, keyCount);
            }
            YeltPartitionMapper mapper = new YeltPartitionMapper(new[] { yeltPartitionLinkedListReader, yeltPartitionLinkedListReader2 }, sortedKeys);

            int j = 0;
            Stopwatch sw = Stopwatch.StartNew();
            int[] rows = null;
            while (j++ <= 100)
            {
                rows = mapper.MapKeys(1).MappedIndices;
                mapper.Reset();
            }
            sw.Stop();
            System.Console.WriteLine(sw);

            System.Console.ReadLine();
        }

        private static decimal GetLimit100Pct(LayerDetail layerDetail)
        {
            return layerDetail.LimitBasis == LimitBasis.Aggregate ?
                layerDetail.AggLimit :
                (layerDetail.LimitBasis == LimitBasis.PerRisk || layerDetail.LimitBasis == LimitBasis.NonCATQuotaShare ?
                    layerDetail.RiskLimit :
                    layerDetail.OccLimit);
        }

        private static RevoSnowflakeRepository GetRevoSnowflakeRepository() => new RevoSnowflakeRepository(new SnowflakeConnectionStrings().RevoBermudaConnectionString);
        private static RevoSqlRepository GetRevoSqlRepository()
        {
            ConnectionProtection connectionProtection =
                new ConnectionProtection(@"C:\Users\atosco\source\repos\Arch.ILS.EconomicModel\Arch.ILS.EconomicModel.Console\App.config.config");
            if (!connectionProtection.IsProtected())
                connectionProtection.EncryptFile();
            RevoConnectionStrings connectionSettings = new RevoConnectionStrings(connectionProtection, false);
            return new RevoSqlRepository(connectionSettings.GetConnectionString(RevoConnectionStrings.REVO));
        }
        private static RevoLayerLossSnowflakeRepository GetRevoLayerLossSnowflakeRepository() => new RevoLayerLossSnowflakeRepository(new SnowflakeConnectionStrings().RevoLayerLossBermudaConnectionString);

        private static RevoLayerLossSqlRepository GetRevoLayerLossSqlRepository()
        {
            ConnectionProtection connectionProtection =
                new ConnectionProtection(@"C:\Users\atosco\source\repos\Arch.ILS.EconomicModel\Arch.ILS.EconomicModel.Console\App.config.config");
            RevoConnectionStrings connectionSettings = new RevoConnectionStrings(connectionProtection, false);
            RevoSqlRepository revoRepository = new RevoSqlRepository(connectionSettings.GetConnectionString(RevoConnectionStrings.REVO));
            return new RevoLayerLossSqlRepository(connectionSettings.GetConnectionString(RevoConnectionStrings.REVOLAYERLOSS));
        }
    }
}

