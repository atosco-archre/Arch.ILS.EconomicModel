
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Arch.ILS.Common;
using Arch.ILS.EconomicModel;
using Google.Apis.Storage.v1.Data;

namespace Arch.ILS.EconomicModel.Console
{
    public class Program
    {
        public static unsafe void Main(string[] args)
        {
            //ExportRetroCessions();
            ExportPremiumByRetroProfile(new DateTime(2024, 9, 30));
            //SetPortfolioLayerCession();
        }

        public static void ExportRetroCessions()
        {
            /*Authentication*/
            ConnectionProtection connectionProtection =
                new ConnectionProtection(@"C:\Users\atosco\source\repos\Arch.ILS.EconomicModel\Arch.ILS.EconomicModel.Console\App.config.config");
            //if (!connectionProtection.IsProtected())
            //    connectionProtection.EncryptFile();
            //RevoSnowflakeRepository revoSnowflakeRepository = new RevoSnowflakeRepository(new SnowflakeConnectionStrings().ConnectionString);
            RevoConnectionStrings connectionSettings = new RevoConnectionStrings(connectionProtection, false);
            RevoSqlRepository revoRepository = new RevoSqlRepository(connectionSettings.GetConnectionString(RevoConnectionStrings.REVO));
            //var retroPrograms = revoRepository.GetRetroPrograms().Result;
            //var retroAllocations = revoRepository.GetRetroAllocations().Result;
            //var spInsurers = revoRepository.GetSPInsurers().Result;
            //var retroprograms = revoRepository.GetRetroPrograms().Result;
            //var retroInvestorResets = revoRepository.GetRetroInvestorResets().Result.ToList();
            //var retroProgramResets = revoRepository.GetRetroProgramResets().Result.ToList();

            //var retroResetCessions = revoRepository.GetRetroResetCessions().Result.ToList();
            //var retroInitialCessions = revoRepository.GetRetroInitialCessions().Result.ToList();
            //var investorResetCessions = revoRepository.GetInvestorResetCessions().Result.ToList();
            //var investorInitialCessions = revoRepository.GetInvestorInitialCessions().Result.ToList();
            var retroAllocationView = revoRepository.GetRetroAllocationView().Result;
            var levelLayerCessions = retroAllocationView.GetLevelLayerCessions();

            using(FileStream fs = new FileStream(@"C:\Data\RetroAllocations.csv", FileMode.Create, FileAccess.Write, FileShare.Read))
            using (StreamWriter sw = new StreamWriter(fs))
            {
                sw.WriteLine($"RetroLevel,RetroProgramId,LayerId,StartInclusive,StartDayOfYear,EndInclusive,EndDayOfYear,NetCession");
                foreach (var c in levelLayerCessions)
                    sw.WriteLine($"{c.RetroLevel},{c.RetroProgramId},{c.LayerId},{c.PeriodCession.StartInclusive},{c.PeriodCession.StartInclusive.DayOfYear},{c.PeriodCession.EndInclusive},{c.PeriodCession.EndInclusive.DayOfYear},{c.PeriodCession.NetCession}");
                sw.Flush();
            }
        }

        public static void ExportPremiumByRetroProfile(DateTime currentFxDate, bool useBoundFx = true, int previousYear = 2023, int currentYear = 2024, string baseCurrency = "USD")
        {
            /*Authentication*/
            ConnectionProtection connectionProtection =
                new ConnectionProtection(@"C:\Users\atosco\source\repos\Arch.ILS.EconomicModel\Arch.ILS.EconomicModel.Console\App.config.config");
            RevoConnectionStrings connectionSettings = new RevoConnectionStrings(connectionProtection, false);
            RevoSqlRepository revoRepository = new RevoSqlRepository(connectionSettings.GetConnectionString(RevoConnectionStrings.REVO));            
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
                if(!submissions.Result.TryGetValue(layerDetail.SubmissionId, out var submission))
                    continue;
                var fxRate = fxRates.Result.GetRate(currentFxDate, baseCurrency, submission.Currency);
                var retroProgram = retroPrograms.Result[layerCession.RetroProgramId];
                if(!retroProfiles.Result.TryGetValue(retroProgram.RetroProfileId, out var retroProfile))
                    continue;
                
                if(!retroProfileUwYearPremium.TryGetValue(retroProfile.RetroProfileId, out var uwYearsPremiums))
                {
                    uwYearsPremiums = new Dictionary<int, decimal>();
                    retroProfileUwYearPremium[retroProfile.RetroProfileId] = uwYearsPremiums;
                }

                decimal cededPremium = layerDetail.Premium
                    * (layerDetail.Placement == decimal.Zero ? decimal.Zero : 1 / layerDetail.Placement)
                    * (layerDetail.SignedShare > decimal.Zero ? layerDetail.SignedShare : layerDetail.EstimatedShare)
                    * fxRate
                    * layerCession.PeriodCession.NetCession
                    * (submission.TranType == TranType.Ceded ? - 1 : (submission.TranType == TranType.Assumed ? 1 : 0))
                    * (decimal)(((layerCession.PeriodCession.EndInclusive - layerCession.PeriodCession.StartInclusive).TotalDays + 1) / ((layerDetail.Expiration - layerDetail.Inception).TotalDays + 1));
                if (uwYearsPremiums.ContainsKey(layerDetail.UWYear))
                    uwYearsPremiums[layerDetail.UWYear] += cededPremium;
                else
                    uwYearsPremiums[layerDetail.UWYear] = cededPremium;
            }

            using (FileStream fs = new FileStream(@"C:\Data\RetroProfilePremiums.csv", FileMode.Create, FileAccess.Write, FileShare.Read))
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

        public static void SetPortfolioLayerCession()
        {
            /*Authentication*/
            ConnectionProtection connectionProtection =
                new ConnectionProtection(@"C:\Users\atosco\source\repos\Arch.ILS.EconomicModel\Arch.ILS.EconomicModel.Console\App.config.config");
            //if (!connectionProtection.IsProtected())
            //    connectionProtection.EncryptFile();
            //RevoSnowflakeRepository revoSnowflakeRepository = new RevoSnowflakeRepository(new SnowflakeConnectionStrings().ConnectionString);
            RevoConnectionStrings connectionSettings = new RevoConnectionStrings(connectionProtection, false);
            RevoSqlRepository revoRepository = new RevoSqlRepository(connectionSettings.GetConnectionString(RevoConnectionStrings.REVO));
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
            ConnectionProtection connectionProtection =
                new ConnectionProtection(@"C:\Users\atosco\source\repos\Arch.ILS.EconomicModel\Arch.ILS.EconomicModel.Console\App.config.config");
            RevoConnectionStrings connectionSettings = new RevoConnectionStrings(connectionProtection, false);
            RevoSqlRepository revoRepository = new RevoSqlRepository(connectionSettings.GetConnectionString(RevoConnectionStrings.REVO));


            RevoLayerLossSqlRepository revoLayerLossSqlRepository = new RevoLayerLossSqlRepository(connectionSettings.GetConnectionString(RevoConnectionStrings.REVOLAYERLOSS));
            var layerYelt = revoLayerLossSqlRepository.GetLayerDayYeltVectorised(10619, 38252).Result;
            YeltPartitioner yeltPartitioner = new YeltPartitioner(new Range[] { new Range(2, 50) }, layerYelt);
            YeltPartitionReader yeltPartitionLinkedListReader = YeltPartitionReader.Initialise(yeltPartitioner);
            var layerYelt2 = revoLayerLossSqlRepository.GetLayerDayYeltVectorised(10620, 38252).Result;
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
    }
}

