
using System;
using System.Buffers;
using System.Data.SqlClient;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

using Arch.ILS.Core;
using Studio.Core;
using Studio.Core.Sql;

namespace Arch.ILS.EconomicModel
{
    public class RevoSqlRepository : SqlRepository, IRevoRepository
    {
        public RevoSqlRepository(string connectionString) : base(connectionString)
        {
        }

        public Task<Dictionary<int, Layer>> GetLayers()
        {
            return Task.Factory.StartNew(() =>
            {
                return ExecuteReaderSql(GET_LAYERS).GetObjects<Layer>().ToDictionary(x => x.LayerId);
            });
        }

        public Task<Dictionary<int, RetroProgram>> GetRetroPrograms()
        {
            return Task.Factory.StartNew(() =>
            {
                return ExecuteReaderSql(GET_RETRO_PROGRAM).GetObjects<RetroProgram>().ToDictionary(x => x.RetroProgramId);
            });
        }

        public Task<Dictionary<int, Portfolio>> GetPortfolios()
        {
            return Task.Factory.StartNew(() =>
            {
                return ExecuteReaderSql(GET_PORTFOLIOS).GetObjects<Portfolio>().ToDictionary(x => x.PortfolioId);
            });
        }

        public Task<Dictionary<int, PortLayer>> GetPortfolioLayers()
        {
            return Task.Factory.StartNew(() =>
            {
                return ExecuteReaderSql(GET_PORTFOLIO_LAYERS).GetObjects<PortLayer>().ToDictionary(x => x.PortLayerId);
            });
        }

        public Task<IEnumerable<PortLayerCession>> GetPortfolioLayerCessions()
        {
            Console.Write("0 - Non Parallel ");
            return Task.Factory.StartNew<IEnumerable<PortLayerCession>>(() =>
            {
                return ExecuteReaderSql(GET_PORTFOLIO_LAYER_CESSIONS).GetObjects<PortLayerCession>();
            });
        }

        public Task<IEnumerable<PortLayerCession>> GetPortfolioLayerCessionsParallel(int partitionCount = 8)
        {
            return Task.Factory.StartNew(() =>
            {
                PortLayerCession[][] portLayerCessions = new PortLayerCession[partitionCount][];
                Task[] portLayerCessionsTasks = new Task[partitionCount];
                for (int i = 0; i < portLayerCessionsTasks.Length; i++)
                    portLayerCessionsTasks[i] = Task.Factory.StartNew(state =>
                    {
                        int index = (int)state!;
                        portLayerCessions[index] = ExecuteReaderSql(string.Format(GET_PORTFOLIO_LAYER_CESSIONS_BY_PARTITION, index, partitionCount)).GetObjects<PortLayerCession>().ToArray();
                    }, i);
                Task.WaitAll(portLayerCessionsTasks);
                return portLayerCessions.SelectMany(x => x);
            });
        }

        public PortfolioRetroCessions GetLayerView(int partitionCount = 8)
        {
            var portLayersTask = GetPortfolioLayers();
            var layersTask = GetLayers();
            var portfoliosTask = GetPortfolios();
            var retroProgramsTask = GetRetroPrograms();
            Task.WaitAll(portLayersTask, layersTask, portfoliosTask, retroProgramsTask);
            Dictionary<int, PortLayer> portLayers = portLayersTask.Result;
            Dictionary<int, Layer> layers = layersTask.Result;
            Dictionary<int, Portfolio> portfolios = portfoliosTask.Result;
            Dictionary<int, RetroProgram> retroPrograms = retroProgramsTask.Result;
            List<PortLayerCessionExtended>[] partitionedPortLayerCessions = new List<PortLayerCessionExtended>[partitionCount];

            for (int i = 0; i < partitionedPortLayerCessions.Length; i++)
                partitionedPortLayerCessions[i] = new();

            Task[] portLayerCessionsTasks = new Task[partitionCount];
            for (int i = 0; i < portLayerCessionsTasks.Length; i++)
                portLayerCessionsTasks[i] = Task.Factory.StartNew(state =>
                {
                    var input = ((int index, List<PortLayerCessionExtended> layerCessionRepo))state!;
                    
                    foreach(var portLayerCession in ExecuteReaderSql(string.Format(GET_PORTFOLIO_LAYER_CESSIONS_BY_PARTITION, input.index, partitionCount)).GetObjects<PortLayerCessionExtended>())
                    {
                        if (!retroPrograms.TryGetValue(portLayerCession.RetroProgramId, out RetroProgram retroProgram))
                            continue;

                        PortLayer portLayer = portLayers[portLayerCession.PortLayerId];
                        Portfolio portfolio = portfolios[portLayer.PortfolioId];
                        Layer layer = layers[portLayer.LayerId];
                        portLayerCession.PortfolioId = portfolio.PortfolioId;
                        portLayerCession.LayerId = layer.LayerId;
                        portLayerCession.RetroLevelType = retroProgram.RetroLevelType;
                        DateTime? inception;
                        if ((inception = GetPortfolioLayerInception(portfolio, layer)) == null)
                            continue;
                        DateTime portLayerInception = (DateTime)inception;
                        DateTime portLayerExpiration = portLayerInception.AddYears(1).AddDays(-1);

                        if (portLayerExpiration < retroProgram.Inception
                            || portLayerInception > retroProgram.Expiration
                            || (retroProgram.RetroProgramType != RetroProgramType.LOD /*1 = LOD*/ && portLayerInception < retroProgram.Inception))/*if RAD, discard ones where the layer started before the retro*/
                            continue;

                        portLayerCession.OverlapStart = retroProgram.RetroProgramType != RetroProgramType.RAD /*2 = RAD*/ && retroProgram.Inception > portLayerInception
                            ? retroProgram.Inception
                            : portLayerInception;
                        portLayerCession.OverlapEnd = retroProgram.RetroProgramType != RetroProgramType.RAD /*2 = RAD*/ && retroProgram.Expiration < portLayerExpiration
                            ? retroProgram.Expiration
                            : portLayerExpiration;

                        input.layerCessionRepo.Add(portLayerCession);
                    }
                }, (i, partitionedPortLayerCessions[i]));
            Task.WaitAll(portLayerCessionsTasks);

            return new(partitionedPortLayerCessions.SelectMany(cession => cession));

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static DateTime? GetPortfolioLayerInception(Portfolio portfolio, Layer layer)
            {
                return portfolio.PortfolioType switch
                {
                    0 => layer.Inception,
                    1 => layer.Inception.AddYears(1),
                    _ when portfolio.PortfolioType == 2 && layer.Inception.Year == portfolio.AsOfDate.Year => layer.Inception,
                    _ when portfolio.PortfolioType == 2 && layer.Inception.Year == portfolio.AsOfDate.Year - 1 => layer.Inception.AddYears(1),
                    _ when portfolio.PortfolioType == 3 && layer.Inception.Year == portfolio.AsOfDate.Year => layer.Inception.AddYears(1),
                    _ when portfolio.PortfolioType == 3 && layer.Inception.Year == portfolio.AsOfDate.Year - 1 => layer.Inception.AddYears(2),
                    _ => null
                };
            }
        }

        public RetroCessions GetRetroAllocationView()
        {
            var retroAllocationsTask = GetRetroAllocations();
            var retroInvestorsTask = GetRetroInvestors();
            var spInsurersTask = GetSPInsurers(); 
            var layerRetroPlacementsTask = GetRetroPlacements();
            var investorResetCessionsTask = GetInvestorResetCessions();
            var investorInitialCessionsTask = GetInvestorInitialCessions();
            var layersTask = GetLayers();
            var retroProgramsTask = GetRetroPrograms();

            Task.WaitAll(investorResetCessionsTask, investorInitialCessionsTask);
            InvestorCession[] investorInitialCessions = investorInitialCessionsTask.Result.ToArray();
            InvestorCession[] investorResetCessions = investorResetCessionsTask.Result.ToArray();
            Dictionary<(int RetroProgramId, int RetroInvestorId), InvestorCession[]> investorRetroCessionPeriods = investorResetCessions.Union(investorInitialCessions.Except(investorResetCessions, new InvestorRetroProgramResetDateComparer()))
                .GroupBy(g => (g.RetroProgramId, g.RetroInvestorId))
                .ToDictionary(k => k.Key, v => v.OrderBy(o => o.StartDate).ToArray());//take investor cessions preferably from the RetroInvestorReset table rather than the RetroInvestor table. 

            Task.WaitAll(retroAllocationsTask, retroInvestorsTask, spInsurersTask, layerRetroPlacementsTask);
            IList<RetroAllocation> retroAllocations = retroAllocationsTask.Result;
            IList<RetroInvestor> retroInvestors = retroInvestorsTask.Result;
            Dictionary<int, SPInsurer> spInsurers = spInsurersTask.Result;
            Dictionary<(int LayerId, int RetroProgramId), LayerRetroPlacement> layerRetroPlacements = layerRetroPlacementsTask.Result
                .ToDictionary(x => (x.LayerId, x.RetroProgramId));

            var retroInvestorPrograms = retroInvestors
                .Join(spInsurers, ok => ok.SPInsurerId, ik => ik.Value.SPInsurerId, (o, i) => new { o, i.Value.RetroProgramId });
            Dictionary<(int RetroProgramId, int LayerId), (decimal GrossCessionBeforePlacement, decimal CalculatedGrossCessionBeforePlacement, decimal Placement, decimal GrossCessionAfterPlacement, int[] RetroInvestors)> retroProgramsLayerGrossAllocation =
                retroAllocations
                .Join(retroInvestorPrograms, ok => ok.RetroInvestorId, ik => ik.o.RetroInvestorId, (o, i) => new { o, i.RetroProgramId })
                .LeftOuterJoin(layerRetroPlacements, ok => (ok.o.LayerId, ok.RetroProgramId), ik => ik.Key, (o, i) => new { o, Placement = i.Value?.Placement ?? 1.0M })
                .LeftOuterJoin(investorRetroCessionPeriods, ok => (ok.o.RetroProgramId, ok.o.o.RetroInvestorId), ik => ik.Key, (o, i) => new { o, i.Value[0].CessionBeforePlacement, i.Key.RetroInvestorId }) //to handle the case of RetroProgram 101 wih Retro Zone Placement = 0 but non zero layer retro cessions
                .GroupBy(g => (g.o.o.RetroProgramId, g.o.o.o.LayerId))
                .ToDictionary(k => k.Key
                    , v => (v.Sum(x => x.CessionBeforePlacement)
                    , v.Sum(x => x.o.Placement == decimal.Zero ? x.CessionBeforePlacement : x.o.o.o.CessionGross / x.o.Placement)
                    , v.Max(x => x.o.Placement), v.Sum(x => x.o.o.o.CessionGross)
                    , v.Select(x => x.RetroInvestorId).ToArray()));

            Dictionary<int, (DateTime StartDate, int RetroProgramResetId)[]> retroProgramResetDates = investorRetroCessionPeriods
                .GroupBy(x => x.Key.RetroProgramId, y => y.Value.Select(z => (z.StartDate, z.RetroProgramResetId)))
                .ToDictionary(k => k.Key, v => v.SelectMany(s => s).Distinct().OrderBy(vv => vv.StartDate).ToArray());
            Dictionary<int, Layer> layers = layersTask.Result;
            Dictionary<int, RetroProgram> retroPrograms = retroProgramsTask.Result;
            List<RetroLayerCession> retroLayerCessions = new();
            /*TODO: Note that there are cases where the CalculatedGrossCessionBeforePlacement is different from GrossCessionBeforePlacement. In all the cases I could check, the CalculatedGrossCessionBeforePlacement was the correct one but keep in mind this mismatch*/
            foreach (var retroGrossAllocation in retroProgramsLayerGrossAllocation.OrderByDescending(x => x.Key.RetroProgramId))
            {
                int retroProgramId = retroGrossAllocation.Key.RetroProgramId;
                (DateTime StartDate, int RetroProgramResetId)[] resetDates = retroProgramResetDates[retroProgramId];
                RetroProgram retroProgram = retroPrograms[retroGrossAllocation.Key.RetroProgramId];

                if (!layers.TryGetValue(retroGrossAllocation.Key.LayerId, out Layer layer)
                    || !TryGetLayerRetroIntersection(layer, retroProgram, out DateTime overlapStart, out DateTime overlapEnd))
                    continue;
                if (resetDates.Length == 1)
                {
                    var initialCession = resetDates[0];
                    retroLayerCessions.Add(new RetroLayerCession
                    {
                        RetroProgramId = retroProgramId,
                        LayerId = retroGrossAllocation.Key.LayerId,
                        RetroProgramResetId = initialCession.RetroProgramResetId,
                        CessionGross = retroGrossAllocation.Value.GrossCessionAfterPlacement,
                        RetroLevelType = retroProgram.RetroLevelType,
                        OverlapStart = overlapStart,
                        OverlapEnd = overlapEnd
                    });
                }
                else
                {
                    int[] retroInvestorIds = retroGrossAllocation.Value.RetroInvestors;
                    InvestorCession[][] investorCessions = new InvestorCession[retroInvestorIds.Length][];
                    for(int j = 0; j < retroInvestorIds.Length; ++j)
                        investorCessions[j] = investorRetroCessionPeriods[(retroProgramId, retroInvestorIds[j])];

                    Dictionary<DateTime, decimal> resetDateGrossCessionAfterPlacement = investorCessions
                        .SelectMany(x => x)
                        .GroupBy(g => g.StartDate)
                        .ToDictionary(k => k.Key, v => v.Sum(vv => vv.CessionBeforePlacement) * (retroGrossAllocation.Value.Placement == decimal.Zero ? retroGrossAllocation.Value.GrossCessionAfterPlacement / retroGrossAllocation.Value.CalculatedGrossCessionBeforePlacement : retroGrossAllocation.Value.Placement));
                    for (int i = 0; i < resetDates.Length; i++)
                    {
                        var resetCession = resetDates[i];
                        DateTime resetStart = resetDates[i].StartDate;
                        if (i == 0 && resetStart != retroProgram.Inception)
                            throw new InvalidDataException("Expected the initial cession date to match the retro inception date");
                        DateTime resetEnd = i + 1 >= resetDates.Length ? retroProgram.Expiration : resetDates[i + 1].StartDate.AddDays(-1);
                        if (!TryGetPeriodIntersection(resetStart, resetEnd, retroProgram.Inception, retroProgram.Expiration, out DateTime retroOverlapStart, out DateTime retroOverlapEnd)
                         || !TryGetLayerRetroIntersection(layer, retroProgram.RetroProgramType, retroOverlapStart, retroOverlapEnd, out DateTime resetOverlapStart, out DateTime resetOverlapEnd))
                            continue;
                        retroLayerCessions.Add(new RetroLayerCession
                        {
                            RetroProgramId = retroProgramId,
                            LayerId = retroGrossAllocation.Key.LayerId,
                            RetroProgramResetId = resetCession.RetroProgramResetId,
                            CessionGross = resetDateGrossCessionAfterPlacement[resetStart],
                            RetroLevelType = retroProgram.RetroLevelType,
                            OverlapStart = resetOverlapStart,
                            OverlapEnd = resetOverlapEnd
                        });
                    }
                }
            }

            return new RetroCessions(retroLayerCessions);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Range[] GetYeltDayRanges(in DateTime inceptionDate, in DateTime expirationDate)
        {
            const int firstDayOfYear = 1;
            const int lastDayOfYear = 365;
            int days = (expirationDate - inceptionDate).Days + 1;
            if (days < 0)
                throw new ArgumentException("Expected an Expiration Date >= Inception Date");
            if (days >= lastDayOfYear)
                return [new Range(firstDayOfYear, lastDayOfYear)];
            if (inceptionDate.DayOfYear > expirationDate.DayOfYear)  //period intersecting two successive calendar years
                return [new Range(firstDayOfYear, expirationDate.DayOfYear), new Range(inceptionDate.DayOfYear, lastDayOfYear)];
            else return [new Range(inceptionDate.DayOfYear, expirationDate.DayOfYear)];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryGetPeriodIntersection(in DateTime inceptionA, in DateTime expirationA, in DateTime inceptionB, in DateTime expirationB, out DateTime overlapStart, out DateTime overlapEnd)
        {
            overlapStart = inceptionA > inceptionB ? inceptionA : inceptionB;
            overlapEnd = expirationA > expirationB ? expirationB : expirationA;
            return overlapStart <= overlapEnd;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryGetLayerRetroIntersection(in Layer layer, in RetroProgram retroProgram, out DateTime overlapStart, out DateTime overlapEnd)
            => TryGetLayerRetroIntersection(layer, retroProgram.RetroProgramType, retroProgram.Inception, retroProgram.Expiration, out overlapStart, out overlapEnd);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryGetLayerRetroIntersection(in Layer layer, in RetroProgramType retroProgramType, in DateTime retroInception, in DateTime retroExpiration, out DateTime overlapStart, out DateTime overlapEnd)
        {
            if (retroProgramType == RetroProgramType.RAD)
            {
                if (layer.Inception < retroInception || layer.Inception > retroExpiration)
                {
                    overlapStart = DateTime.MinValue;
                    overlapEnd = DateTime.MinValue;
                    return false;
                }
                else
                {
                    overlapStart = layer.Inception;
                    overlapEnd = layer.Expiration;
                    return true;
                }
            }
            else if (retroProgramType == RetroProgramType.LOD)
            {
                overlapStart = layer.Inception > retroInception ? layer.Inception : retroInception;
                overlapEnd = layer.Expiration > retroExpiration ? retroExpiration : layer.Expiration;
                return overlapStart <= overlapEnd;
            }
            else throw new NotImplementedException(retroProgramType.ToString());
        }


        public Task<IList<RetroAllocation>> GetRetroAllocations()
        {
            return Task.Factory.StartNew(() =>
            {
                IList<RetroAllocation> retroAllocations = new List<RetroAllocation>();
                using (var reader = (SqlDataReader)ExecuteReaderSql(string.Format(GET_RETRO_ALLOCATION)))
                {
                    while (reader.Read())
                    {
                        int index = 0;
                        RetroAllocation retroAllocation = new()
                        {
                            RetroAllocationId = reader.GetInt32(index),
                            //ROL = reader.GetDecimal(++index),
                            //EL = reader.GetDecimal(++index),
                            //Zone = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //Message = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            LayerId = reader.GetInt32(++index),
                            RetroInvestorId = reader.GetInt32(++index),
                            //CreateDate = reader.GetDateTime(++index),
                            //CreateUser = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //ModifyDate = reader.GetDateTime(++index),
                            //ModifyUser = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //IsActive = reader.GetBoolean(++index),
                            //IsDeleted = reader.GetBoolean(++index),
                            //RegisStatus = reader.GetInt32(++index),
                            //RegisMessage = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            CessionNet = reader.GetDecimal(++index),
                            //CessionDemand = reader.GetDecimal(++index),
                            CessionGross = reader.GetDecimal(++index),
                            RowVersion = reader.GetInt64(++index),
                            CessionCapFactor = reader.GetDecimal(++index),
                            //CessionCapFactorSent = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //CessionGrossFinalSent = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //CessionNetFinalSent = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //AllocationStatus = reader.GetInt32(++index),
                            Override = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            Brokerage = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            Taxes = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //OverrideSent = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //BrokerageSent = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //TaxesSent = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            ManagementFee = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            TailFee = reader.IsDBNull(++index) ? null : reader.GetDecimal(index),
                            //IsPortInExpiredLayer = reader.GetBoolean(++index),
                            TopUpZoneId = reader.IsDBNull(++index) ? null : reader.GetInt32(index),
                            CessionPlaced = reader.GetDecimal(++index),
                        };

                        retroAllocations.Add(retroAllocation);
                    }
                }

                return retroAllocations;
            });
        }

        public Task<IEnumerable<RetroInvestorReset>> GetRetroInvestorResets()
        {
            return Task.Factory.StartNew(() =>
            {
                return ExecuteReaderSql(GET_RETRO_INVESTOR_RESET).GetObjects<RetroInvestorReset>();
            });
        }

        public Task<IEnumerable<RetroProgramReset>> GetRetroProgramResets()
        {
            return Task.Factory.StartNew(() =>
            {
                return ExecuteReaderSql(GET_RETRO_PROGRAM_RESET).GetObjects<RetroProgramReset>();
            });
        }

        public Task<Dictionary<int, SPInsurer>> GetSPInsurers()
        {
            return Task.Factory.StartNew(() =>
            {
                Dictionary<int, SPInsurer> spInsurers = new Dictionary<int, SPInsurer>();
                using (var reader = (SqlDataReader)ExecuteReaderSql(string.Format(GET_SPINSURER)))
                {
                    while (reader.Read())
                    {
                        int index = 0;
                        SPInsurer spInsurer = new()
                        {
                            SPInsurerId = reader.GetInt32(index),
                            RetroProgramId = reader.GetInt32(++index),
                            //SegregatedAccount = reader.GetString(++index),
                            //ContractId = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //InsurerId = reader.GetInt32(++index),
                            //TrustBank = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //CreateDate = reader.GetDateTime(++index),
                            //CreateUser = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //ModifyDate = reader.GetDateTime(++index),
                            //ModifyUser = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //IsActive = reader.GetBoolean(++index),
                            //IsDeleted = reader.GetBoolean(++index),
                            RowVersion = reader.GetInt64(++index),
                            //TrustAccountNumber = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //FundsWithheldAccountNumber = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            InitialCommutationDate = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                            FinalCommutationDate = reader.IsDBNull(++index) ? null : reader.GetDateTime(index),
                        };

                        spInsurers[spInsurer.SPInsurerId] = spInsurer;
                    }
                }

                return spInsurers;
            });
        }

        public Task<IList<RetroInvestor>> GetRetroInvestors()
        {
            return Task.Factory.StartNew(() =>
            {
                IList<RetroInvestor> retroInvestors = new List<RetroInvestor>();
                using (var reader = (SqlDataReader)ExecuteReaderSql(string.Format(GET_RETRO_INVESTOR)))
                {
                    while (reader.Read())
                    {
                        int index = 0;
                        RetroInvestor retroInvestor = new()
                        {
                            RetroInvestorId = reader.GetInt32(index),
                            SPInsurerId = reader.GetInt32(++index),
                            //Name = reader.GetString(++index),
                            Status = reader.GetInt32(++index),
                            TargetCollateral = reader.GetDecimal(++index),
                            NotionalCollateral = reader.GetDecimal(++index),
                            InvestmentEstimated = reader.GetDecimal(++index),
                            InvestmentAuth = reader.GetDecimal(++index),
                            InvestmentSigned = reader.GetDecimal(++index),
                            InvestmentEstimatedAmt = reader.GetDecimal(++index),
                            InvestmentAuthAmt = reader.GetDecimal(++index),
                            InvestmentSignedAmt = reader.GetDecimal(++index),
                            ExcludedFacilities = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            ExcludedLayerSubNos = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            ExcludedDomiciles = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            IsFundsWithheld = reader.GetBoolean(++index),
                            RetroCommissionId = reader.GetInt32(++index),
                            //CreateDate = reader.GetDateTime(++index),
                            //CreateUser = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //ModifyDate = reader.GetDateTime(++index),
                            //ModifyUser = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            //IsActive = reader.GetBoolean(++index),
                            //IsDeleted = reader.GetBoolean(++index),
                            //RuleDefs = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            RowVersion = reader.GetInt64(++index),
                            ExcludedLayerIds = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            TargetPremium = reader.GetDecimal(++index),
                            Override = reader.GetDecimal(++index),
                            ManagementFee = reader.GetDecimal(++index),
                            ProfitComm = reader.GetDecimal(++index),
                            PerformanceFee = reader.GetDecimal(++index),
                            RHOE = reader.GetDecimal(++index),
                            HurdleRate = reader.GetDecimal(++index),
                            IsPortIn = reader.GetBoolean(++index),
                            IsPortOut = reader.GetBoolean(++index),
                            RetroBufferType = reader.GetInt32(++index),
                            CessionCapBufferPct = reader.GetDecimal(++index),
                            RetroValuesToBuffer = reader.IsDBNull(++index) ? null : reader.GetString(index),
                            ExcludedContractType = reader.GetInt32(++index),
                        };

                        retroInvestors.Add(retroInvestor);
                    }
                }

                return retroInvestors;
            });
        }

        public Task<IEnumerable<RetroZone>> GetRetroZones()
        {
            return Task.Factory.StartNew(() =>
            {
                return ExecuteReaderSql(GET_RETRO_ZONE).GetObjects<RetroZone>();
            });
        }

        public Task<IEnumerable<LayerTopUpZone>> GetLayerTopUpZones()
        {
            return Task.Factory.StartNew(() =>
            {
                return ExecuteReaderSql(GET_LAYER_TOPUPZONE).GetObjects<LayerTopUpZone>();
            });
        }

        public Task<IEnumerable<RetroCession>> GetRetroResetCessions()
        {
            return Task.Factory.StartNew(() =>
            {
                var retroInvestorsResetTask = GetRetroInvestorResets();
                var retroProgramResetTask = GetRetroProgramResets();
                Task.WaitAll(retroInvestorsResetTask, retroProgramResetTask);
                var retroInvestorsResets = retroInvestorsResetTask.Result;
                var retroProgramResets = retroProgramResetTask.Result;
                return retroInvestorsResets
                    .Join(retroProgramResets, ok => ok.RetroProgramResetId, ik => ik.RetroProgramResetId, (o, i) => new { i.RetroProgramId, i.StartDate, i.RetroProgramResetId, o })
                    .GroupBy(g => (g.RetroProgramId, g.StartDate, g.RetroProgramResetId))
                    .Select(x => new RetroCession(x.Key.RetroProgramResetId, x.Key.RetroProgramId, x.Key.StartDate, x.Sum(oo => oo.o.InvestmentSignedAmt), x.Max(oo => oo.o.TargetCollateral), x.Sum(oo => oo.o.InvestmentSigned)))
                    //.Where(r => r.CessionBeforePlacement != 0)
                    ;
            });
        }

        public Task<IEnumerable<RetroCession>> GetRetroInitialCessions()
        {
            return Task.Factory.StartNew(() =>
            {
                var retroInvestorsTask = GetRetroInvestors();
                var spInsurersTask = GetSPInsurers();
                var retroProgramTask = GetRetroPrograms();
                Task.WaitAll(retroInvestorsTask, spInsurersTask, retroProgramTask);
                var retroInvestors = retroInvestorsTask.Result;
                var spInsurers = spInsurersTask.Result;
                var retroPrograms = retroProgramTask.Result;
                return retroInvestors
                    .Join(spInsurers, ri => ri.SPInsurerId, spi => spi.Key, (ri, spi) => new { spi.Value.RetroProgramId , ri })
                    .GroupBy(temp => temp.RetroProgramId)
                    .Join(retroPrograms, ok => ok.Key, ik => ik.Key, (o, i) => new RetroCession(-1, i.Value.RetroProgramId, i.Value.Inception, o.Sum(oo => oo.ri.InvestmentSignedAmt), o.Max(oo => oo.ri.TargetCollateral), o.Sum(oo => oo.ri.InvestmentSigned)))
                    //.Where(r => r.CessionBeforePlacement != 0)
                    ;
            });
        }

        public Task<IEnumerable<InvestorCession>> GetInvestorResetCessions()
        {
            return Task.Factory.StartNew(() =>
            {
                var retroInvestorsResetTask = GetRetroInvestorResets();
                var retroProgramResetTask = GetRetroProgramResets();
                Task.WaitAll(retroInvestorsResetTask, retroProgramResetTask);
                var retroInvestorsResets = retroInvestorsResetTask.Result;
                var retroProgramResets = retroProgramResetTask.Result;
                return retroInvestorsResets
                    .Join(retroProgramResets, ok => ok.RetroProgramResetId, ik => ik.RetroProgramResetId, (o, i) => new { i.RetroProgramId, i.StartDate, i.RetroProgramResetId, o })
                    .GroupBy(g => (g.RetroProgramId, g.StartDate, g.RetroProgramResetId, g.o.RetroInvestorId))
                    .Select(x => new InvestorCession(x.Key.RetroInvestorId, x.Key.RetroProgramResetId, x.Key.RetroProgramId, x.Key.StartDate, x.Sum(oo => oo.o.InvestmentSignedAmt), x.Max(oo => oo.o.TargetCollateral), x.Sum(oo => oo.o.InvestmentSigned)))
                    //.Where(r => r.CessionBeforePlacement != 0)
                    ;
            });
        }

        public Task<IEnumerable<InvestorCession>> GetInvestorInitialCessions()
        {
            return Task.Factory.StartNew(() =>
            {
                var retroInvestorsTask = GetRetroInvestors();
                var spInsurersTask = GetSPInsurers();
                var retroProgramTask = GetRetroPrograms();
                Task.WaitAll(retroInvestorsTask, spInsurersTask, retroProgramTask);
                var retroInvestors = retroInvestorsTask.Result;
                var spInsurers = spInsurersTask.Result;
                var retroPrograms = retroProgramTask.Result;
                return retroInvestors
                    .Join(spInsurers, ri => ri.SPInsurerId, spi => spi.Key, (ri, spi) => new { spi.Value.RetroProgramId, ri })
                    .GroupBy(temp => (temp.RetroProgramId, temp.ri.RetroInvestorId))
                    .Join(retroPrograms, ok => ok.Key.RetroProgramId, ik => ik.Key, (o, i) => new InvestorCession(o.Key.RetroInvestorId, -1, i.Value.RetroProgramId, i.Value.Inception, o.Sum(oo => oo.ri.InvestmentSignedAmt), o.Max(oo => oo.ri.TargetCollateral), o.Sum(oo => oo.ri.InvestmentSigned)))
                    //.Where(r => r.CessionBeforePlacement != 0)
                    ;
            });
        }

        public Task<IEnumerable<LayerRetroPlacement>> GetRetroPlacements()
        {
            return Task.Factory.StartNew(() =>
            {
                var layerTopUpZonesTask = GetLayerTopUpZones();
                var retroZonesTask = GetRetroZones();
                Task.WaitAll(layerTopUpZonesTask, retroZonesTask);
                IEnumerable<LayerTopUpZone> layerTopUpZones = layerTopUpZonesTask.Result;
                RetroZone[] retroZones = retroZonesTask.Result.ToArray();
                var retroZonePlacements = retroZones.GroupBy(g => (g.RetroProgramId, g.TopUpZoneId))
                    .Select(x => new { x.Key, MaxCession = x.Max(xx => xx.Cession), MinCession = x.Min(xx => xx.Cession) });

                if (retroZonePlacements.Any(x => x.MaxCession != x.MinCession))
                    throw new NotImplementedException("Difference Placements for the same zone and retro program at different dates not handled,");

                return layerTopUpZones
                    .Join(retroZonePlacements, ok => ok.TopUpZoneId, ik => ik.Key.TopUpZoneId, (o, i) => new LayerRetroPlacement(o.LayerId, i.Key.RetroProgramId, i.MaxCession ))                    
                    ;
            });
        }

        #region Types

        private class InvestorRetroProgramResetDateComparer : IEqualityComparer<InvestorCession>
        {
            public bool Equals([DisallowNull] InvestorCession x, [DisallowNull] InvestorCession y)
            {
                return x.RetroInvestorId == y.RetroInvestorId
                    && x.RetroProgramId == y.RetroProgramId
                    && x.StartDate == y.StartDate;
            }

            public int GetHashCode([DisallowNull] InvestorCession obj)
            {
                return obj.RetroInvestorId ^ obj.RetroProgramId ^ obj.StartDate.GetHashCode();
            }
        }

        #endregion Types
        #region Constants

        private const string GET_LAYERS = @"SELECT LayerId
     , Inception
     , Expiration
  FROM dbo.Layer
 WHERE IsActive = 1
   AND IsDeleted = 0";

        private const string GET_PORTFOLIOS = @"SELECT PortfolioId
     , PortfolioType
     , AsOfDate
  FROM dbo.Portfolio
 WHERE IsActive = 1
   AND IsDeleted = 0";

        private const string GET_PORTFOLIO_LAYERS = @"SELECT PortLayerId
     , LayerId
     , PortfolioId
  FROM dbo.PortLayer
 WHERE IsActive = 1
   AND IsDeleted = 0";

        private const string GET_RETRO_PROGRAM = @"SELECT RetroProgramId
     , Inception
     , Expiration
     , CONVERT(TINYINT, RetroProgramType) AS RetroProgramType
     , CONVERT(TINYINT, RetroLevelType + 1) AS RetroLevelType
  FROM dbo.RetroProgram
 WHERE /*Status IN (22,10)/*remove projection retros*/
   AND */IsActive = 1
   AND IsDeleted = 0";

        private const string GET_PORTFOLIO_LAYER_CESSIONS = @"SELECT PortLayerCessionId
     , PortLayerId
     , RetroProgramId
     , CessionGross
     /*, CessionNet*/
  FROM dbo.PortLayerCession
 WHERE IsActive = 1
   AND ShouldCessionApply = 1
   AND IsDeleted = 0
   AND CessionGross > 0";

        private const string GET_PORTFOLIO_LAYER_CESSIONS_BY_PARTITION = @"SELECT PortLayerCessionId
     , PortLayerId
     , RetroProgramId
     , CessionGross
     /*, CessionNet*/
  FROM dbo.PortLayerCession
 WHERE (PortLayerCessionId % {1}) = {0} 
   AND IsActive = 1
   AND ShouldCessionApply = 1
   AND IsDeleted = 0
   AND CessionGross > 0";

        private const string GET_RETRO_ALLOCATION = @"SELECT RetroAllocationId
      --,ROL
      --,EL
      --,Zone
      --,Message
      ,LayerId
      ,RetroInvestorId
      --,CreateDate
      --,CreateUser
      --,ModifyDate
      --,ModifyUser
      --,IsActive
      --,IsDeleted
      --,RegisStatus
      --,RegisMessage
      ,CessionNet
      --,CessionDemand
      ,CessionGross
      ,CONVERT(BIGINT, RowVersion) AS RowVersion
      ,CessionCapFactor
      --,CessionCapFactorSent
      --,CessionGrossFinalSent
      --,CessionNetFinalSent
      --,AllocationStatus
      ,Override
      ,Brokerage
      ,Taxes
      --,OverrideSent
      --,BrokerageSent
      --,TaxesSent
      ,ManagementFee
      ,TailFee
      --,IsPortInExpiredLayer
      ,TopUpZoneId
      ,CessionPlaced
  FROM dbo.RetroAllocation
 WHERE IsActive = 1
   AND IsDeleted = 0;";

        private const string GET_RETRO_INVESTOR_RESET = @"SELECT RetroInvestorResetId
      ,RetroInvestorId
      ,RetroProgramResetId
      ,StartDate
      ,TargetCollateral
      ,TargetPremium
      ,InvestmentSignedAmt
      ,InvestmentSigned
      ,CONVERT(BIGINT, RowVersion) AS RowVersion
      --,CreateDate
      --,CreateUser
      --,ModifyDate
      --,ModifyUser
      --,IsActive
      --,IsDeleted
  FROM dbo.RetroInvestorReset
 WHERE IsActive = 1
  AND IsDeleted = 0";

        private const string GET_RETRO_PROGRAM_RESET = @"SELECT RetroProgramResetId
      ,RetroProgramId
      ,StartDate
      ,TargetCollateral
      ,TargetPremium
      ,CONVERT(BIGINT, RowVersion) AS RowVersion
      --,CreateDate
      --,CreateUser
      --,ModifyDate
      --,ModifyUser
      --,IsActive
      --,IsDeleted
  FROM dbo.RetroProgramReset
 WHERE IsActive = 1
   AND IsDeleted = 0";

        private const string GET_SPINSURER = @"SELECT SPInsurerId
      ,RetroProgramId
      --,SegregatedAccount
      --,ContractId
      --,InsurerId
      --,TrustBank
      --,CreateDate
      --,CreateUser
      --,ModifyDate
      --,ModifyUser
      --,IsActive
      --,IsDeleted
      ,CONVERT(BIGINT, RowVersion) AS RowVersion
      --,TrustAccountNumber
      --,FundsWithheldAccountNumber
      ,InitialCommutationDate
      ,FinalCommutationDate
  FROM dbo.SPInsurer
 WHERE IsActive = 1
   AND IsDeleted = 0";


        private const string GET_RETRO_INVESTOR = @"SELECT RetroInvestorId
      ,SPInsurerId
      --,Name
      ,Status
      ,TargetCollateral
      ,NotionalCollateral
      ,InvestmentEstimated
      ,InvestmentAuth
      ,InvestmentSigned
      ,InvestmentEstimatedAmt
      ,InvestmentAuthAmt
      ,InvestmentSignedAmt
      ,ExcludedFacilities
      ,ExcludedLayerSubNos
      ,ExcludedDomiciles
      ,IsFundsWithheld
      ,RetroCommissionId
      --,CreateDate
      --,CreateUser
      --,ModifyDate
      --,ModifyUser
      --,IsActive
      --,IsDeleted
      --,RuleDefs
      ,CONVERT(BIGINT, RowVersion) AS RowVersion
      ,ExcludedLayerIds
      ,TargetPremium
      ,Override
      ,ManagementFee
      ,ProfitComm
      ,PerformanceFee
      ,RHOE
      ,HurdleRate
      ,IsPortIn
      ,IsPortOut
      ,RetroBufferType
      ,CessionCapBufferPct
      ,RetroValuesToBuffer
      ,ExcludedContractType
  FROM dbo.RetroInvestor
 WHERE IsActive = 1
   AND IsDeleted = 0";

        private const string GET_RETRO_ZONE = @"SELECT --RetroZoneId,
       RetroProgramId
      --,Name
      --,ELLowerBound
      --,ELUpperBound
      --,ROLLowerBound
      --,ROLUpperBound
      ,Cession
      --,CessionCap
      --,CreateDate
      --,CreateUser
      --,ModifyDate
      --,ModifyUser
      --,IsActive
      --,IsDeleted
      --,RowVersion
      --,CessionCapAdjusted
      ,TopUpZoneId
      --,StartDate
  FROM dbo.RetroZone
 WHERE IsActive = 1
   AND IsDeleted = 0";

        private const string GET_LAYER_TOPUPZONE = @"SELECT LayerId
      ,TopUpZoneId
  FROM dbo.Layer
 WHERE IsActive = 1
   AND IsDeleted = 0
   AND TopUpZoneId IS NOT NULL";

        #endregion Constants
    }
}
