
namespace Arch.ILS.EconomicModel
{
    public record class LimitMetrics
    {
        public LimitMetrics(in decimal subjectLimit, in decimal subjectLimitPlaced, in decimal cededLimit)
        {
            SubjectLimit = subjectLimit;
            SubjectLimitPlaced = subjectLimitPlaced;
            CededLimit = cededLimit;
        }
        public decimal SubjectLimit { get; set; }
        public decimal SubjectLimitPlaced { get; set; }
        public decimal CededLimit { get; set; }
    }
}
