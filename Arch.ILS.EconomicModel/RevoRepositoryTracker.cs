
using Arch.ILS.EconomicModel.Repositories;

namespace Arch.ILS.EconomicModel
{
    public class RevoRepositoryTracker
    {
        public const int DEFAULT_TIMER_DUETIME_IN_MILLISECONDS = 0;
        public const int DEFAULT_TIMER_PERIOD_IN_MILLISECONDS = 60000;

        private readonly IRevoRepository _revoRepository;
        private readonly Dictionary<RevoDataTable, long> _retroCessionViewTableLatestRowVersion;
        private Timer _timer;

        public RevoRepositoryTracker(IRevoRepository revoRepository)
        {
            _revoRepository = revoRepository;
            _retroCessionViewTableLatestRowVersion = new Dictionary<RevoDataTable, long>();
        }

        public RetroCessions LatestRetroCessions { get; private set; }

        public Task Initialise()
        {
            return Task.Factory.StartNew(() =>
            {
                InitialiseRetroCessionViewTracker();
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

        public Task Synchronise()
        {
            return Task.Factory.StartNew(() =>
            {
                if (RetroCessionViewChanged())
                {
                    LatestRetroCessions = _revoRepository.GetRetroCessionView().Result;
                }
               
            });
        }

        private async void InitialiseRetroCessionViewTracker()
        {
            Task<long>[] tasks = CreateRetroCessionViewTableTrackerTask();
            Task.WaitAll(tasks);
            int index = 0;
            _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroAllocation] = tasks[index].Result;
            _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroInvestor] = tasks[++index].Result;
            _retroCessionViewTableLatestRowVersion[RevoDataTable.SPInsurer] = tasks[++index].Result;
            _retroCessionViewTableLatestRowVersion[RevoDataTable.Layer] = tasks[++index].Result;
            _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroProgram] = tasks[++index].Result;
            _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroZone] = tasks[++index].Result;
            _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroInvestorReset] = tasks[++index].Result;
            _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroProgramReset] = tasks[++index].Result;
        }

        private bool RetroCessionViewChanged()
        {
            Task<long>[] tasks = CreateRetroCessionViewTableTrackerTask();
            Task.WaitAll(tasks);
            bool changed = false;
            int index = 0;
            long newRetroAllocationRowVersion = tasks[index].Result;
            if(newRetroAllocationRowVersion > _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroAllocation])
            {
                _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroAllocation] = newRetroAllocationRowVersion;
                changed = true;
            }

            long newRetroInvestorRowVersion = tasks[++index].Result;
            if (newRetroInvestorRowVersion > _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroInvestor])
            {
                _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroInvestor] = newRetroInvestorRowVersion;
                changed = true;
            }

            long newSPInsurerRowVersion = tasks[++index].Result;
            if (newSPInsurerRowVersion > _retroCessionViewTableLatestRowVersion[RevoDataTable.SPInsurer])
            {
                _retroCessionViewTableLatestRowVersion[RevoDataTable.SPInsurer] = newSPInsurerRowVersion;
                changed = true;
            }

            long newLayerRowVersion = tasks[++index].Result;
            if (newLayerRowVersion > _retroCessionViewTableLatestRowVersion[RevoDataTable.Layer])
            {
                _retroCessionViewTableLatestRowVersion[RevoDataTable.Layer] = newLayerRowVersion;
                changed = true;
            }

            long newRetroProgramRowVersion = tasks[++index].Result;
            if (newRetroProgramRowVersion > _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroProgram])
            {
                _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroProgram] = newRetroProgramRowVersion;
                changed = true;
            }

            long newRetroZoneRowVersion = tasks[++index].Result;
            if (newRetroZoneRowVersion > _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroZone])
            {
                _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroZone] = newRetroZoneRowVersion;
                changed = true;
            }

            long newRetroInvestorResetRowVersion = tasks[++index].Result;
            if (newRetroInvestorResetRowVersion > _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroInvestorReset])
            {
                _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroInvestorReset] = newRetroInvestorResetRowVersion;
                changed = true;
            }

            long newRetroProgramResetRowVersion = tasks[++index].Result;
            if (newRetroProgramResetRowVersion > _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroProgramReset])
            {
                _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroProgramReset] = newRetroProgramResetRowVersion;
                changed = true;
            }
           
            return changed;
        }

        private Task<long>[] CreateRetroCessionViewTableTrackerTask()
        {
            return
            [
                _revoRepository.GetLatestRowVersion(RevoDataTable.RetroAllocation),
                _revoRepository.GetLatestRowVersion(RevoDataTable.RetroInvestor),
                _revoRepository.GetLatestRowVersion(RevoDataTable.SPInsurer),
                _revoRepository.GetLatestRowVersion(RevoDataTable.Layer),
                _revoRepository.GetLatestRowVersion(RevoDataTable.RetroProgram),
                _revoRepository.GetLatestRowVersion(RevoDataTable.RetroZone),
                _revoRepository.GetLatestRowVersion(RevoDataTable.RetroInvestorReset),
                _revoRepository.GetLatestRowVersion(RevoDataTable.RetroProgramReset)
            ];
        }
    }
}
