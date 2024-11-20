
namespace Arch.ILS.EconomicModel
{
    public unsafe ref struct YeltDayPartition
    {
        public YeltDayPartition(ReadOnlySpan<short> days, ReadOnlySpan<long> yearDayPerilIdEventIdKeys, ReadOnlySpan<double> lossPcts, ReadOnlySpan<double> rps, ReadOnlySpan<double> rbs) 
        {
            PartitionDays = days;
            PartitionYearDayPerilIdEventIdKeys = yearDayPerilIdEventIdKeys;
            PartitionLossPcts = lossPcts;
            PartitionRPs = rps;
            PartitionRBs = rbs;
        }
        public ReadOnlySpan<short> PartitionDays;
        public ReadOnlySpan<long> PartitionYearDayPerilIdEventIdKeys;
        public ReadOnlySpan<double> PartitionLossPcts;
        public ReadOnlySpan<double> PartitionRPs;
        public ReadOnlySpan<double> PartitionRBs;
    }
}
