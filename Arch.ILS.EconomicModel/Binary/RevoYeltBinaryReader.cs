
namespace Arch.ILS.EconomicModel.Binary
{
    public class RevoYeltBinaryReader
    {
        public RevoYeltBinaryReader(in string filePath)
        {
            FilePath = filePath;
        }

        public string FilePath { get; }

        public IYelt ReadAll(bool parallelise = true)
        {
            return new RevoLayerDayYeltVectorised2(FilePath, parallelise);
        }
    }
}
