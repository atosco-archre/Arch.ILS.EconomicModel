
using Studio.Core;

namespace Arch.ILS.EconomicModel
{
    public sealed class RetroProgram : IRecord
    {
        [Field(0)]
        public int RetroProgramId { get; set; }
        [Field(1)]
        public int RetroProfileId { get; set; }
        [Field(2)]
        public DateTime Inception { get; set; }
        [Field(3)]
        public DateTime Expiration { get; set; }
        [Field(4)]
        public RetroProgramType RetroProgramType { get; set; }
        [Field(5)]
        public byte RetroLevelType { get; set; }
    }
}
