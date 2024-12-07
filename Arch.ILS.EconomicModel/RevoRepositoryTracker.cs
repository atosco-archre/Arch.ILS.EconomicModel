using Arch.ILS.EconomicModel.Repositories;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

        private void InitialiseRetroCessionViewTracker()
        {
            _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroAllocation] = _revoRepository.GetLatestRowVersion(RevoDataTable.RetroAllocation);
            _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroInvestor] = _revoRepository.GetLatestRowVersion(RevoDataTable.RetroInvestor);
            _retroCessionViewTableLatestRowVersion[RevoDataTable.SPInsurer] = _revoRepository.GetLatestRowVersion(RevoDataTable.SPInsurer);
            _retroCessionViewTableLatestRowVersion[RevoDataTable.Layer] = _revoRepository.GetLatestRowVersion(RevoDataTable.Layer);
            _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroProgram] = _revoRepository.GetLatestRowVersion(RevoDataTable.RetroProgram);
            _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroZone] = _revoRepository.GetLatestRowVersion(RevoDataTable.RetroZone);
            _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroInvestorReset] = _revoRepository.GetLatestRowVersion(RevoDataTable.RetroInvestorReset);
            _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroProgramReset] = _revoRepository.GetLatestRowVersion(RevoDataTable.RetroProgramReset);
        }

        private bool RetroCessionViewChanged()
        {
            bool changed = false;
            long newRetroAllocationRowVersion = _revoRepository.GetLatestRowVersion(RevoDataTable.RetroAllocation);
            if(newRetroAllocationRowVersion > _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroAllocation])
            {
                _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroAllocation] = newRetroAllocationRowVersion;
                changed = true;
            }

            long newRetroInvestorRowVersion = _revoRepository.GetLatestRowVersion(RevoDataTable.RetroInvestor);
            if (newRetroInvestorRowVersion > _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroInvestor])
            {
                _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroInvestor] = newRetroInvestorRowVersion;
                changed = true;
            }

            long newSPInsurerRowVersion = _revoRepository.GetLatestRowVersion(RevoDataTable.SPInsurer);
            if (newSPInsurerRowVersion > _retroCessionViewTableLatestRowVersion[RevoDataTable.SPInsurer])
            {
                _retroCessionViewTableLatestRowVersion[RevoDataTable.SPInsurer] = newSPInsurerRowVersion;
                changed = true;
            }

            long newLayerRowVersion = _revoRepository.GetLatestRowVersion(RevoDataTable.Layer);
            if (newLayerRowVersion > _retroCessionViewTableLatestRowVersion[RevoDataTable.Layer])
            {
                _retroCessionViewTableLatestRowVersion[RevoDataTable.Layer] = newLayerRowVersion;
                changed = true;
            }

            long newRetroProgramRowVersion = _revoRepository.GetLatestRowVersion(RevoDataTable.RetroProgram);
            if (newRetroProgramRowVersion > _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroProgram])
            {
                _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroProgram] = newRetroProgramRowVersion;
                changed = true;
            }

            long newRetroZoneRowVersion = _revoRepository.GetLatestRowVersion(RevoDataTable.RetroZone);
            if (newRetroZoneRowVersion > _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroZone])
            {
                _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroZone] = newRetroZoneRowVersion;
                changed = true;
            }

            long newRetroInvestorResetRowVersion = _revoRepository.GetLatestRowVersion(RevoDataTable.RetroInvestorReset);
            if (newRetroInvestorResetRowVersion > _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroInvestorReset])
            {
                _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroInvestorReset] = newRetroInvestorResetRowVersion;
                changed = true;
            }

            long newRetroProgramResetRowVersion = _revoRepository.GetLatestRowVersion(RevoDataTable.RetroProgramReset);
            if (newRetroProgramResetRowVersion > _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroProgramReset])
            {
                _retroCessionViewTableLatestRowVersion[RevoDataTable.RetroProgramReset] = newRetroProgramResetRowVersion;
                changed = true;
            }

            return changed;
        }
    }
}
