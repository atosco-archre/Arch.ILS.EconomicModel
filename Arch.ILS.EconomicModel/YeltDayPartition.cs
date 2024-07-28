
namespace Arch.ILS.EconomicModel
{
    public unsafe ref struct YeltDayPartition
    {
        public YeltDayPartition(ReadOnlySpan<short> days, ReadOnlySpan<long> yearDayEventIdKeys, ReadOnlySpan<double> lossPcts, ReadOnlySpan<double> rps, ReadOnlySpan<double> rbs) 
        {
            PartitionDays = days;
            PartitionYearDayEventIdKeys = yearDayEventIdKeys;
            PartitionLossPcts = lossPcts;
            PartitionRPs = rps;
            PartitionRBs = rbs;
        }
        public ReadOnlySpan<short> PartitionDays;
        public ReadOnlySpan<long> PartitionYearDayEventIdKeys;
        public ReadOnlySpan<double> PartitionLossPcts;
        public ReadOnlySpan<double> PartitionRPs;
        public ReadOnlySpan<double> PartitionRBs;
    }
}
