
using System.Runtime.CompilerServices;

using Studio.Core;

namespace Arch.ILS.EconomicModel
{
    public class RevoGULossRepository : IRevoGULossRepository
    {
        #region Variables

        private readonly IRepository _repository;

        #endregion Variables

        #region Constructor

        public RevoGULossRepository(IRepository repository)
        {
            _repository = repository;
        }

        #endregion Constructor

        #region Methods

        public Task<IEnumerable<RevoGUYeltEntry>> GetRevoGUYelt(int guAnalysisId)
        {
            return Task.Factory.StartNew(() =>
            {
                return GetRevoGUYeltEntries(guAnalysisId);
            });
        }

        public Task<IEnumerable<RevoGUYeltEntry>> GetRevoGUYelt(in int guLossAnalysisId, int partitionCount)
        {
            return Task.Factory.StartNew<IEnumerable<RevoGUYeltEntry>>((id) =>
            {
                int analysisId = (int)id!;
                Task[] yeltPartitionsTasks = new Task[partitionCount];
                IEnumerable<RevoGUYeltEntry>[] partitionEntries = new IEnumerable<RevoGUYeltEntry>[partitionCount];
                for (int i = 0; i < yeltPartitionsTasks.Length; i++)
                    yeltPartitionsTasks[i] = Task.Factory.StartNew(state =>
                    {
                        (int partitionAnalysisId, int partitionId, int partitionsCount) = ((int partitionAnalysisId, int partitionId, int partitionsCount))state!;
                        partitionEntries[partitionId] = GetRevoGUYeltEntries(partitionAnalysisId, partitionId, partitionsCount);
                    }, (analysisId, i, partitionCount));
                Task.WaitAll(yeltPartitionsTasks);
                return partitionEntries.SelectMany(x => x);
            }, guLossAnalysisId);
        }

        private IEnumerable<RevoGUYeltEntry> GetRevoGUYeltEntries(int guAnalysisId)
        {
            using (var reader = _repository.ExecuteReaderSql(string.Format(GET_GU_YELT_QUERY, guAnalysisId)))
            {
                while (reader.Read())
                {
                    yield return new RevoGUYeltEntry
                    {
                        Year = (short)reader.GetInt32(0),
                        EventId = reader.GetInt64(1),
                        Peril = Enum.Parse<RevoPeril>(reader.GetString(2)),
                        Day = reader.GetInt16(3),
                        Loss = reader.GetDouble(4)
                    };
                }

                reader.Close();
            }
        }

        private IEnumerable<RevoGUYeltEntry> GetRevoGUYeltEntries(int guAnalysisId, int partitionId, int partitionCount)
        {
            using (var reader = _repository.ExecuteReaderSql(string.Format(GET_GU_YELT_QUERY_BY_PARTITION, guAnalysisId, partitionCount, partitionId)))
            {
                while (reader.Read())
                {
                    yield return new RevoGUYeltEntry
                    {
                        Year = (short)reader.GetInt32(0),
                        EventId = reader.GetInt64(1),
                        Peril = Enum.Parse<RevoPeril>(reader.GetString(2)),
                        Day = reader.GetInt16(3),
                        Loss = reader.GetDouble(4)
                    };
                }

                reader.Close();
            }
        }


        #endregion Methods

        #region Constants

        private const string GET_GU_YELT_QUERY = @"SELECT Year 
     , EventId
     , Peril
     , Day
     , SUM(Loss) AS Loss
  FROM dbo.GUYelt
 WHERE GUAnalysisId = {0}
 GROUP BY Year 
     , EventId
     , Peril
     , Day";

        private const string GET_GU_YELT_QUERY_BY_PARTITION = @"SELECT Year 
     , EventId
     , Peril
     , Day
     , SUM(Loss) AS Loss
  FROM dbo.GUYelt
 WHERE GUAnalysisId = {0}
   AND (Year % {1}) = {2}
 GROUP BY Year 
     , EventId
     , Peril
     , Day";


        #endregion Constants
    }
}
