
namespace Arch.ILS.EconomicModel
{
    public class YeltFileInfoFactory
    {
        public static string GetFileNameWithExtension(in int lossAnalysisId, in int layerId, in long rowVersion) => $"Yelt_{lossAnalysisId}_{layerId}_{rowVersion}.bin";
        public static string GetFileNamePrefix(in int lossAnalysisId, in int layerId) => $"Yelt_{lossAnalysisId}_{layerId}_";
    }
}
