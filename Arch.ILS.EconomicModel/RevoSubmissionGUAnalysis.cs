
using Studio.Core;

namespace Arch.ILS.EconomicModel
{
    public class RevoSubmissionGUAnalysis : IRecord
    {
        [Field(0)]
        public int GUAnalysisId { get; set; }
        [Field(1)]
        public int SubmissionId { get; set; }
        [Field(2)]
        public double FXRate { get; set; }
        [Field(3)]
        public DateTime FXDate { get; set; }
        [Field(4)]
        public long RowVersion { get; set; }
    }
}
