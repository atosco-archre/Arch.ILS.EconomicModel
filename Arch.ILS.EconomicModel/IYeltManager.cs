
namespace Arch.ILS.EconomicModel
{
    public interface IYeltManager
    {
        ViewType ViewType { get; }

        Task Initialise(bool pauseRepoUpdate = false);

        Task Synchronise(bool pauseRepoUpdate = false);

        void ScheduleSynchronisation(int dueTimeInMilliseconds, int periodInMilliseconds);

        void CancelSchedule();

        bool TryGetLatestLayerLossAnalysis(in int layerId, in RevoLossViewType revoLossViewType, out LayerLossAnalysis layerLossAnalysis);
    }
}
