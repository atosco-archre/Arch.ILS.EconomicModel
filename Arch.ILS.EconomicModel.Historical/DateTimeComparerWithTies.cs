
namespace Arch.ILS.EconomicModel.Historical
{
    public class DateTimeComparerWithTies : IComparer<DateTime>
    {
        public int Compare(DateTime x, DateTime y)
        {
            int compare = x.CompareTo(y);
            return compare == 0 ? 1 : compare;
        }
    }
}
