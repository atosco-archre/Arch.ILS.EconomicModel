
namespace Arch.ILS.EconomicModel
{
    public interface IRetroLayerRepository
    {
        Task<IEnumerable<RetroLayer>> GetRetroLayers();

        Task<IEnumerable<RetroLayer>> GetRetroLayers(long afterRowVersion);
    }
}
