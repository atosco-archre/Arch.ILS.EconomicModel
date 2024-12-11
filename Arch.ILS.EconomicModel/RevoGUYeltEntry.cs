
namespace Arch.ILS.EconomicModel
{
    public class RevoGUYeltEntry
    {
        public RevoGUYeltEntry()
        {
        }

        public short Year { get; set; }

        public long EventId { get; set; }

        public RevoPeril Peril { get; set; }

        public short Day { get; set; }

        public double Loss { get; set; }
    }
}
