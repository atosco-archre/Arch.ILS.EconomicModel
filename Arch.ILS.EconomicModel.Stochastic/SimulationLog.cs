
using System.Text;

namespace Arch.ILS.EconomicModel.Stochastic
{
    public class SimulationLog
    {
        private readonly Dictionary<(int, LogLevel), (bool isRetroLayer, StringBuilder message)> _layerLogs;
        private readonly List<(LogLevel, string)> _generalMessages;

        public SimulationLog() 
        {
            _layerLogs = new();
            _generalMessages = new();
        }

        public void Append(LogLevel logLevel, in int layerId, in string message, in bool isRetroLayer)
        {
            if (!_layerLogs.TryGetValue((layerId, logLevel), out var layerLogInfo))
            {
                layerLogInfo = (isRetroLayer, new());
                _layerLogs.Add((layerId, logLevel), layerLogInfo);
            }
            layerLogInfo.message.Append(message);
        }

        public void Append(LogLevel logLevel, in string message)
        {
            _generalMessages.Add((logLevel, message));
        }

        public void Export(string logFilePath)
        {
            using (FileStream fs = new(logFilePath, FileMode.Create, FileAccess.Write, FileShare.None))
            using (StreamWriter sw = new(fs))
            {
                sw.WriteLine("LOGLEVEL,LAYERID,ISRETROLAYER,MESSAGE");
                foreach (var kv in _layerLogs.OrderBy(x => x.Key))
                {
                    var k = kv.Key;
                    var v = kv.Value;
                    sw.WriteLine($"{k.Item2},{k.Item1},{v.isRetroLayer},{v.message.ToString()}");
                }

                foreach (var pair in _generalMessages)
                {
                    sw.WriteLine($"{pair.Item1},,,{pair.Item2}");
                }

                sw.Flush();
            }
        }
    }
}
