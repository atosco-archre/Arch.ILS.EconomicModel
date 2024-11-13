
using Studio.Core;

namespace Arch.ILS.EconomicModel
{
    public class Reinstatement : IRecord
    {
        [Field(0)]
        public int ReinstatementId { get; set; }

        [Field(1)]
        public int LayerId { get; set; }

        [Field(2)]
        public int ReinstatementOrder { get; set; }

        [Field(3)]
        public double Quantity { get; set; }

        [Field(4)]
        public decimal PremiumShare { get; set; }

        [Field(5)]
        public decimal BrokeragePercentage { get; set; }

        [Field(6)]
        public long RowVersion { get; set; }

        //[Field(7)]
        //public bool IsProRata { get; set; }    
    }
}
