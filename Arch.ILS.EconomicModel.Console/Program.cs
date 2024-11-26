
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Amazon.S3.Model;
using Arch.ILS.Common;
using Arch.ILS.EconomicModel;
using Arch.ILS.EconomicModel.Binary;
using Arch.ILS.Snowflake;
using Google.Apis.Storage.v1.Data;

namespace Arch.ILS.EconomicModel.Console
{
    public class Program
    {
        public static unsafe void Main(string[] args)
        {
            //ExportRetroLayerCessions(@"C:\Data\RetroAllocations LOD Resets.csv", ResetType.LOD);
            //ExportRetroCessionMetrics(@"C:\Data\RetroMetrics.csv", new DateTime(2024, 9, 30), true);
            //ExportRetroLayerCessionMetrics(@"C:\Data\RetroLayerMetrics_RADReset.csv", new DateTime(2024, 9, 30), ResetType.RAD, true);
            //ExportPremiumByRetroProfile(@"C:\Data\RetroProfilePremiums_BoundFx.csv", new DateTime(2024, 9, 30), true);
            //ExportPremiumByRetroProfile(@"C:\Data\RetroProfilePremiums_20240930Fx.csv", new DateTime(2024, 9, 30), false);
            //SetPortfolioLayerCession();

            //ProcessLayerYelts();
            UploadRetroYelts(new HashSet<int> { 274 });
        }

        public static void UploadRetroYelts(HashSet<int> retroProgramId)
        {
            IRevoRepository revoRepository = GetRevoSnowflakeRepository();
            IRevoLayerLossRepository revoLayerLossRepository = GetRevoLayerLossSnowflakeRepository();
            RetroLayerYeltManager retroLayerYeltManager = new RetroLayerYeltManager(@"C:\Data\Revo_Yelts", revoRepository, revoLayerLossRepository, retroProgramId);
            retroLayerYeltManager.Initialise();
            retroLayerYeltManager.ScheduleSynchronisation();
            Thread.Sleep(140000);
            retroLayerYeltManager.Dispose();
            System.Console.ReadLine();
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
            var retroAllocationView = revoRepository.GetRetroAllocationView(resetType).Result;
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
            var retroAllocationView = revoRepository.GetRetroAllocationView();
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

        public static void ExportRetroCessionMetrics(string outputFilePath, DateTime currentFxDate, bool useBoundFx = true, string baseCurrency = "USD")
        {
            /*Authentication*/
            RevoRepository revoRepository = GetRevoSnowflakeRepository();
            var retroAllocationView = revoRepository.GetRetroAllocationView();
            var retroPrograms = revoRepository.GetRetroPrograms();
            var layerDetails = revoRepository.GetLayerDetails();
            var submissions = revoRepository.GetSubmissions();
            var fxRates = revoRepository.GetFXRates();

            Task.WaitAll(retroAllocationView, retroPrograms, layerDetails, submissions, fxRates);
            Dictionary<int, RetroMetrics> retroMetricsById = new();
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

                if (!retroMetricsById.TryGetValue(retroProgram.RetroProgramId, out var retroMetrics))
                {
                    retroMetrics = new RetroMetrics(retroProgram.RetroProgramId);
                    retroMetricsById[retroProgram.RetroProgramId] = retroMetrics;
                }

                decimal cededPremium = layerDetail.Premium
                    * (layerDetail.Placement == decimal.Zero ? decimal.Zero : 1 / layerDetail.Placement)
                    * (layerDetail.SignedShare > decimal.Zero ? layerDetail.SignedShare : layerDetail.EstimatedShare)
                    * fxRate
                    * layerCession.PeriodCession.NetCession
                    * (submission.TranType == TranType.Ceded ? -1 : (submission.TranType == TranType.Assumed ? 1 : 0))
                    * (decimal)(((layerCession.PeriodCession.EndInclusive - layerCession.PeriodCession.StartInclusive).TotalDays + 1) / ((layerDetail.Expiration - layerDetail.Inception).TotalDays + 1));
                retroMetrics.CededPremium += cededPremium;

                decimal limit100Pct = GetLimit100Pct(layerDetail);
                decimal cededLimit = limit100Pct
                    * (layerDetail.SignedShare > decimal.Zero ? layerDetail.SignedShare : layerDetail.EstimatedShare)
                    * fxRate
                    * layerCession.PeriodCession.NetCession
                    * (submission.TranType == TranType.Ceded ? -1 : (submission.TranType == TranType.Assumed ? 1 : 0));

                var currentDate = layerCession.PeriodCession.StartInclusive;
                var dateCededLimits = retroMetrics.DateCededLimits;
                while (currentDate <= layerCession.PeriodCession.EndInclusive)
                {
                    if (dateCededLimits.ContainsKey(currentDate))
                        dateCededLimits[currentDate] += cededLimit;
                    else
                        dateCededLimits[currentDate] = cededLimit;
                    currentDate.AddDays(1);
                }
            }

            using (FileStream fs = new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
            using (StreamWriter sw = new StreamWriter(fs))
            {
                sw.WriteLine($"RetroProgramId,CededPremium,CededLimit");
                foreach (KeyValuePair<int, RetroMetrics> retroMetrics in retroMetricsById.OrderBy(x => x.Key))
                {
                    sw.WriteLine($"{retroMetrics.Key},{retroMetrics.Value.CededPremium},{retroMetrics.Value.DateCededLimits.Values.Max()}");
                }
                sw.Flush();
            }
        }

        public static void ExportRetroLayerCessionMetrics(string outputFilePath, DateTime currentFxDate, ResetType resetType, bool useBoundFx = true, string baseCurrency = "USD")
        {
            /*Authentication*/
            RevoRepository revoRepository = GetRevoSnowflakeRepository();
            var retroAllocationView = revoRepository.GetRetroAllocationView(resetType);
            var retroPrograms = revoRepository.GetRetroPrograms();
            var layerDetails = revoRepository.GetLayerDetails();
            var submissions = revoRepository.GetSubmissions();
            var fxRates = revoRepository.GetFXRates();

            Task.WaitAll(retroAllocationView, retroPrograms, layerDetails, submissions, fxRates);
            List<RetroLayerMetrics> retroLayerMetrics = new();
            var levelLayerCessions = retroAllocationView.Result.GetLevelLayerCessions();

            foreach (LayerPeriodCession layerCession in levelLayerCessions)
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

                decimal cededPremium = layerDetail.Premium
                    * (layerDetail.Placement == decimal.Zero ? decimal.Zero : 1 / layerDetail.Placement)
                    * (layerDetail.SignedShare > decimal.Zero ? layerDetail.SignedShare : layerDetail.EstimatedShare)
                    * fxRate
                    * layerCession.PeriodCession.NetCession
                    * (submission.TranType == TranType.Ceded ? -1 : (submission.TranType == TranType.Assumed ? 1 : 0))
                    * (decimal)(((layerCession.PeriodCession.EndInclusive - layerCession.PeriodCession.StartInclusive).TotalDays + 1) / ((layerDetail.Expiration - layerDetail.Inception).TotalDays + 1));

                decimal limit100Pct = GetLimit100Pct(layerDetail);
                decimal cededLimit = limit100Pct
                    * (layerDetail.SignedShare > decimal.Zero ? layerDetail.SignedShare : layerDetail.EstimatedShare)
                    * fxRate
                    * layerCession.PeriodCession.NetCession
                    * (submission.TranType == TranType.Ceded ? -1 : (submission.TranType == TranType.Assumed ? 1 : 0));

                retroLayerMetrics.Add(new RetroLayerMetrics(layerCession.RetroProgramId, layerCession.LayerId, layerCession.PeriodCession.StartInclusive, layerCession.PeriodCession.EndInclusive, cededPremium, cededLimit));
            }

            using (FileStream fs = new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
            using (StreamWriter sw = new StreamWriter(fs))
            {
                sw.WriteLine($"RetroLevel,RetroProgramId,RetroProgramType,RetroInceptionDate,RetroExpirationDate,LayerId,LayerInceptionDate,LayerExpirationDate,CessionStartDateInclusive,CessionEndDateInclusive,CededPremium,CededLimit");
                foreach (RetroLayerMetrics retroLayerMetric in retroLayerMetrics)
                {
                    var retro = retroPrograms.Result[retroLayerMetric.RetroProgramid];
                    var layer = layerDetails.Result[retroLayerMetric.LayerId];
                    sw.WriteLine($"{retro.RetroLevelType},{retroLayerMetric.RetroProgramid},{retro.RetroProgramType.ToString()},{retro.Inception},{retro.Expiration},{retroLayerMetric.LayerId},{layer.Inception},{layer.Expiration},{layer.Status.ToString()},{retroLayerMetric.StartDateInclusive},{retroLayerMetric.EndDateInclusive},{retroLayerMetric.CededPremium},{retroLayerMetric.CededLimit}");
                }
                sw.Flush();
            }
        }

        public static void SetPortfolioLayerCession()
        {
            /*Authentication*/
            RevoRepository revoRepository = GetRevoSnowflakeRepository();
            /*Queries*/
            var Layers2 = revoRepository.GetLayers().Result;
            var portfolios = revoRepository.GetPortfolios().Result;
            var portLayers = revoRepository.GetPortfolioLayers().Result;
            var result = revoRepository.GetLayerView().Result;
            var x = result.GetPortfolioLevelLayerCessions().Where(x => x.RetroProgramId == 218).ToArray();
            var xd = result.GetPortfolioLevelLayerCessions().Where(x => x.RetroProgramId == 218 && x.PortLayerId == 750703).ToArray();
            var portLayersCessions2 = revoRepository.GetPortfolioLayerCessionsParallel().Result.ToArray();
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
                rows = mapper.MapKeys();
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

        #region Types

        internal record class RetroMetrics
        {
            public RetroMetrics(int retroProgramid)
            {
                RetroProgramId = retroProgramid;
                CededPremium = decimal.Zero;
                DateCededLimits = new Dictionary<DateTime, decimal>();
            }

            public int RetroProgramId { get; }
            public decimal CededPremium { get; set; } 
            public Dictionary<DateTime, decimal> DateCededLimits { get; }
        }

        internal record class RetroLayerMetrics(int RetroProgramid, int LayerId, DateTime StartDateInclusive, DateTime EndDateInclusive, decimal CededPremium, decimal CededLimit);

        #endregion Types
    }
}

