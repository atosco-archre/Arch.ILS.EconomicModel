
namespace Arch.ILS.EconomicModel.Benchmark
{
    public unsafe class RevoLayerYeltFixedDictionary
    {
        public const int YEARCOUNT = 10000;

        public RevoLayerYeltFixedDictionary(in int lossAnalysisId, in int layerId, in IEnumerable<RevoLayerYeltEntry> yelt)
        {
            LossAnalysisId = lossAnalysisId;
            LayerId = layerId;
            FixedDictionary<Int16Span, SortedSet<RevoLayerYeltEntry>> fixedDictionary = new();
            foreach(var entry in yelt)
            {
                ref SortedSet<RevoLayerYeltEntry> s = ref fixedDictionary.GetValueRefOrAddDefault(new Int16Span(entry.GetYear()));
                s.Add(entry);
            }
            Yelt = fixedDictionary;
        }
        public int LossAnalysisId { get; }
        public int LayerId { get; }

        public FixedDictionary<Int16Span, SortedSet<RevoLayerYeltEntry>> Yelt { get; }
        //public short[]
    }
}
