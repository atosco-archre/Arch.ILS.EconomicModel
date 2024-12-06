
namespace Arch.ILS.EconomicModel
{
    public interface ILayerRepository
    {
        #region Layer Info

        Task<Dictionary<int, Layer>> GetLayers();

        Task<Dictionary<int, LayerDetail>> GetLayerDetails();

        Task<Dictionary<int, LayerMetaInfo>> GetLayerMetaInfos();

        Task<IEnumerable<Reinstatement>> GetLayerReinstatements();

        Task<IEnumerable<LayerTopUpZone>> GetLayerTopUpZones();

        Task<IDictionary<int, Submission>> GetSubmissions();

        #endregion LayerInfo
    }
}
