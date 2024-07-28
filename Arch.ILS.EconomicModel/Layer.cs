
using Studio.Core;

namespace Arch.ILS.EconomicModel
{
    public class Layer : IRecord
    {
        [Field(0)]
        public int LayerId { get; set; }
        [Field(1)]
        public DateTime Inception { get; set; }
    }
}
