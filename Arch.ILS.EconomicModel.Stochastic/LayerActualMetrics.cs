
namespace Arch.ILS.EconomicModel.Stochastic
{
    public class LayerActualMetrics
    {
        public string MasterKey { get; set; }
        public string MasterKeyFrom { get; set; }
        public int LayerId { get; set; }
        public int SubmissionId { get; set; }
        public bool IsMultiYear { get; set; }
        public bool IsCancellable { get; set; }
        public int UWYear { get; set; }
        public string Segment { get; set; }
        public RegisPerspectiveType PerspectiveType { get; set; }
        public string Currency { get; set; }
        public string Facility { get; set; }
        public double WP { get; set; }
        public double WPxRP { get; set; }
        public double RP { get; set; }
        public double EP { get; set; }
        public double UltLoss { get; set; }
        public double LimitPctUsed { get; set; }

        public static IEnumerable<LayerActualMetrics> LoadFromCsv(string filePath, bool skipFirstLine = true)
        {
            using (FileStream fs = new(filePath, FileMode.Open, FileAccess.Read, FileShare.Read))
            using (StreamReader sr = new(fs))
            {
                if (skipFirstLine)
                    sr.ReadLine();

                string line = null;
                while ((line = sr.ReadLine()) != null)
                {
                    string[] cells = line.Split(',');
                    int index = 0;
                    yield return new LayerActualMetrics
                    {
                        MasterKey = cells[index],
                        MasterKeyFrom = cells[++index],
                        LayerId = int.Parse(cells[++index]),
                        SubmissionId = int.Parse(cells[++index]),
                        IsMultiYear = bool.Parse(cells[++index]),
                        IsCancellable = bool.Parse(cells[++index]),
                        UWYear = int.Parse(cells[++index]),
                        Segment = cells[++index],
                        PerspectiveType = (RegisPerspectiveType)byte.Parse(cells[++index]),
                        Currency = cells[++index],
                        Facility = cells[++index],
                        WP = double.Parse(cells[++index]),
                        WPxRP = double.Parse(cells[++index]),
                        RP = double.Parse(cells[++index]),
                        EP = double.Parse(cells[++index]),
                        UltLoss = double.Parse(cells[++index]),
                        LimitPctUsed = double.Parse(cells[++index])
                    };
                }
            }
        }
    }
}
