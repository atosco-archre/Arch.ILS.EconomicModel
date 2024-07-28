
namespace Arch.EconomicModel.Benchmark
{
    public interface IRevoLayerLossUploadRepository
    {
        #region Methods

        Task<RevoLayerYeltFixedDictionary> GetLayerYeltFixedDictionary();

        Task<RevoLayerYeltYearArray> GetLayerYeltYearArray();

        Task<RevoLayerYeltStandard> GetLayerYeltStandard();

        Task<RevoLayerYeltStandardUnmanaged> GetLayerYeltStandardUnmanaged();

        Task<RevoLayerYeltStandardUnsafe> GetLayerYeltStandardUnsafe();

        Task<RevoLayerYeltUnmanaged> GetLayerYeltUnmanaged();

        #endregion Methods
    }
}
