
using Arch.ILS.EconomicModel.Repositories;

namespace Arch.ILS.EconomicModel
{
    public interface IRevoRepository : ILayerRepository, IRetroInfoRepository, IRetroCessionRepository, IPortfolioInfoRepository, IPortfolioRetroLayerRepository, ILayerLossAnalysisRepository, IRetroLayerRepository, ICurrencyRepository, IRevoTrackerRepository
    {
    }
}
