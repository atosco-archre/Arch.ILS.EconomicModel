
namespace Arch.ILS.EconomicModel
{
    public interface IRevoLayerLossRepository
    {
        #region Methods

        Task<RevoLayerDayYeltVectorised2> GetLayerDayYeltVectorised(in int lossAnalysisId, in int layerId);

        Task<RevoLayerDayYeltVectorised2> GetLayerDayYeltVectorised(in int lossAnalysisId, in int layerId, int partitionCount);

        #endregion Methods
    }
}
