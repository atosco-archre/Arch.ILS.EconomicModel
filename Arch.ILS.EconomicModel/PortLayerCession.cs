
using Studio.Core;

namespace Arch.EconomicModel
{
    public sealed class PortLayerCession : IRecord
    {
        [Field(0)]
        public int PortLayerCessionId { get; set; }
        [Field(1)]
        public int PortLayerId { get; set; }
        [Field(2)]
        public int RetroProgramId { get; set; }
        [Field(3)]
        public decimal CessionGross {  get; set; }
        //[Field(4)]
        //public decimal CessionNet {  get; set; }
    }
}
