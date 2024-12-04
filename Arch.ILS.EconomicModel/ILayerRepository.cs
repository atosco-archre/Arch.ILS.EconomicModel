
namespace Arch.ILS.EconomicModel
{
    public interface ILayerRepository
    {
        Task<Dictionary<int, Layer>> GetLayers();
        Task<Dictionary<int, LayerDetail>> GetLayerDetails();
        Task<Dictionary<int, LayerMetaInfo>> GetLayerMetaInfos();
    }
}
