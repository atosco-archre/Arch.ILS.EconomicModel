using Arch.ILS.Core;
using Studio.Core;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Arch.ILS.EconomicModel
{
    public class RevoLayerLossRepository : IRevoLayerLossRepository
    {
        #region Variables

        private readonly IRepository _repository;

        #endregion Variables

        #region Constructor

        public RevoLayerLossRepository(IRepository repository) 
        {
            _repository = repository;
        }

        #endregion Constructor

        #region Methods

        //public Task<RevoLayerYeltFixedDictionary> GetLayerYeltFixedDictionary(in int lossAnalysisId, in int layerId)
        //{
        //    return Task.Factory.StartNew<RevoLayerYeltFixedDictionary>((ids) =>
        //    {
        //        (int analysisId, int layerId) = ((int, int))ids!;
        //        return new(analysisId, layerId, GetRevoLayerYeltEntries(analysisId, layerId));
        //    }, (lossAnalysisId, layerId));
        //}

        //public Task<IEnumerable<RevoLayerYeltFixedDictionary>> GetLayerYeltsFixedDictionary(IList<(int, int)> lossAnalysisIdLayerIds)
        //{
        //    ArgumentNullException.ThrowIfNull(lossAnalysisIdLayerIds);
        //    return Task.Factory.StartNew((input) =>
        //    {
        //        IList<(int, int)> ids = (IList<(int, int)>)input!;
        //        Task<RevoLayerYeltFixedDictionary>[] tasks = new Task<RevoLayerYeltFixedDictionary>[ids.Count];
        //        for (int i = 0; i < tasks.Length; ++i)
        //        {
        //            tasks[i] = GetLayerYeltFixedDictionary(ids[i].Item1, ids[i].Item2);
        //        }
        //        Task.WaitAll(tasks);
        //        return tasks.Select(x => x.Result);
        //    }, lossAnalysisIdLayerIds);
        //}

        //public Task<RevoLayerYeltYearArray> GetLayerYeltYearArray(in int lossAnalysisId, in int layerId)
        //{
        //    return Task.Factory.StartNew<RevoLayerYeltYearArray>((ids) =>
        //    {
        //        (int analysisId, int layerId) = ((int, int))ids!;
        //        return new(analysisId, layerId, GetRevoLayerYeltEntries(analysisId, layerId));
        //    }, (lossAnalysisId, layerId));
        //}

        //public Task<IEnumerable<RevoLayerYeltYearArray>> GetLayerYeltsYearArray(IList<(int, int)> lossAnalysisIdLayerIds)
        //{
        //    ArgumentNullException.ThrowIfNull(lossAnalysisIdLayerIds);
        //    return Task.Factory.StartNew((input) =>
        //    {
        //        IList<(int, int)> ids = (IList<(int, int)>)input!;
        //        Task<RevoLayerYeltYearArray>[] tasks = new Task<RevoLayerYeltYearArray>[ids.Count];
        //        for (int i = 0; i < tasks.Length; ++i)
        //        {
        //            tasks[i] = GetLayerYeltYearArray(ids[i].Item1, ids[i].Item2);
        //        }
        //        Task.WaitAll(tasks);
        //        return tasks.Select(x => x.Result);
        //    }, lossAnalysisIdLayerIds);
        //}

        //public Task<RevoLayerYeltStandard> GetLayerYeltStandard(in int lossAnalysisId, in int layerId)
        //{
        //    return Task.Factory.StartNew<RevoLayerYeltStandard>((ids) =>
        //    {
        //        (int analysisId, int layerId) = ((int, int))ids!;
        //        return new(analysisId, layerId, GetRevoLayerYeltEntries(analysisId, layerId));
        //    }, (lossAnalysisId, layerId));
        //}

        //public Task<IEnumerable<RevoLayerYeltStandard>> GetLayerYeltsStandard(IList<(int, int)> lossAnalysisIdLayerIds)
        //{
        //    ArgumentNullException.ThrowIfNull(lossAnalysisIdLayerIds);
        //    return Task.Factory.StartNew((input) =>
        //    {
        //        IList<(int, int)> ids = (IList<(int, int)>)input!;
        //        Task<RevoLayerYeltStandard>[] tasks = new Task<RevoLayerYeltStandard>[ids.Count];
        //        for (int i = 0; i < tasks.Length; ++i)
        //        {
        //            tasks[i] = GetLayerYeltStandard(ids[i].Item1, ids[i].Item2);
        //        }
        //        Task.WaitAll(tasks);
        //        return tasks.Select(x => x.Result);
        //    }, lossAnalysisIdLayerIds);
        //}

        //public Task<RevoLayerYeltStandardUnmanaged> GetLayerYeltStandardUnmanaged(in int lossAnalysisId, in int layerId)
        //{
        //    return Task.Factory.StartNew<RevoLayerYeltStandardUnmanaged>((ids) =>
        //    {
        //        (int analysisId, int layerId) = ((int, int))ids!;
        //        return new(analysisId, layerId, GetRevoLayerYeltEntries(analysisId, layerId));
        //    }, (lossAnalysisId, layerId));
        //}

        //public Task<IEnumerable<RevoLayerYeltStandardUnmanaged>> GetLayerYeltsStandardUnmanaged(IList<(int, int)> lossAnalysisIdLayerIds)
        //{
        //    ArgumentNullException.ThrowIfNull(lossAnalysisIdLayerIds);
        //    return Task.Factory.StartNew((input) =>
        //    {
        //        IList<(int, int)> ids = (IList<(int, int)>)input!;
        //        Task<RevoLayerYeltStandardUnmanaged>[] tasks = new Task<RevoLayerYeltStandardUnmanaged>[ids.Count];
        //        for (int i = 0; i < tasks.Length; ++i)
        //        {
        //            tasks[i] = GetLayerYeltStandardUnmanaged(ids[i].Item1, ids[i].Item2);
        //        }
        //        Task.WaitAll(tasks);
        //        return tasks.Select(x => x.Result);
        //    }, lossAnalysisIdLayerIds);
        //}

        //public Task<RevoLayerYeltStandardUnsafe> GetLayerYeltStandardUnsafe(in int lossAnalysisId, in int layerId)
        //{
        //    return Task.Factory.StartNew<RevoLayerYeltStandardUnsafe>((ids) =>
        //    {
        //        (int analysisId, int layerId) = ((int, int))ids!;
        //        return new(analysisId, layerId, GetRevoLayerYeltEntries(analysisId, layerId));
        //    }, (lossAnalysisId, layerId));
        //}

        //public Task<IEnumerable<RevoLayerYeltStandardUnsafe>> GetLayerYeltsStandardUnsafe(IList<(int, int)> lossAnalysisIdLayerIds)
        //{
        //    ArgumentNullException.ThrowIfNull(lossAnalysisIdLayerIds);
        //    return Task.Factory.StartNew((input) =>
        //    {
        //        IList<(int, int)> ids = (IList<(int, int)>)input!;
        //        Task<RevoLayerYeltStandardUnsafe>[] tasks = new Task<RevoLayerYeltStandardUnsafe>[ids.Count];
        //        for (int i = 0; i < tasks.Length; ++i)
        //        {
        //            tasks[i] = GetLayerYeltStandardUnsafe(ids[i].Item1, ids[i].Item2);
        //        }
        //        Task.WaitAll(tasks);
        //        return tasks.Select(x => x.Result);
        //    }, lossAnalysisIdLayerIds);
        //}

        public Task<RevoLayerDayYeltVectorised2> GetLayerYelVectorised(in int lossAnalysisId, in int layerId)
        {
            return Task.Factory.StartNew<RevoLayerDayYeltVectorised2>((ids) =>
            {
                (int analysisId, int layerId) = ((int, int))ids!;
                return new(analysisId, layerId, GetRevoLayerYeltEntries(analysisId, layerId));
            }, (lossAnalysisId, layerId));
        }

        public Task<RevoLayerDayYeltVectorised2> GetLayerDayYeltVectorised(in int lossAnalysisId, in int layerId)
        {
            return Task.Factory.StartNew<RevoLayerDayYeltVectorised2>((ids) =>
            {
                (int analysisId, int layerId) = ((int, int))ids!;
                return new(analysisId, layerId, GetRevoLayerYeltEntries(analysisId, layerId));
            }, (lossAnalysisId, layerId));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private IEnumerable<RevoLayerYeltEntry> GetRevoLayerYeltEntries(int lossAnalysisId, int layerId)
        {
            using (var reader = _repository.ExecuteReaderSql(string.Format(GET_MODELLED_LAYER_YELT_QUERY, lossAnalysisId, layerId)))
            {
                while (reader.Read())
                {
                    yield return new RevoLayerYeltEntry
                    {
                        Year = (short)reader.GetInt32(0),
                        EventId = reader.GetInt32(1),
                        PerilId = (byte)Enum.Parse<RevoPeril>(reader.GetString(2)),
                        Day = reader.GetInt16(3),
                        LossPct = reader.GetDouble(4),
                        RP = reader.GetDouble(5),
                        RB = reader.GetDouble(6),
                    };
                }
            }
        }

        public Task<RevoLayerDayYeltVectorised2> GetLayerDayYeltVectorised(in int lossAnalysisId, in int layerId, int partitionCount)
        {
            return Task.Factory.StartNew<RevoLayerDayYeltVectorised2>((ids) =>
            {
                (int analysisId, int layerId) = ((int, int))ids!;
                Task[] yeltPartitionsTasks = new Task[partitionCount];
                IEnumerable<RevoLayerYeltEntry>[] partitionEntries = new IEnumerable<RevoLayerYeltEntry>[partitionCount];
                for (int i = 0; i < yeltPartitionsTasks.Length; i++)
                    yeltPartitionsTasks[i] = Task.Factory.StartNew(state => 
                    {
                        (int partitionAnalysisId, int partitionLayerId, int partitionId, int partitionsCount) = ((int partitionAnalysisId, int partitionLayerId, int partitionId, int partitionsCount)) state!;
                        partitionEntries[partitionId] = GetRevoLayerYeltEntries(partitionAnalysisId, partitionLayerId, partitionId, partitionsCount);
                    }, (analysisId, layerId, i, partitionCount));
                Task.WaitAll(yeltPartitionsTasks);
                return new(analysisId, layerId, partitionEntries.SelectMany(x => x));
            }, (lossAnalysisId, layerId));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private IEnumerable<RevoLayerYeltEntry> GetRevoLayerYeltEntries(int lossAnalysisId, int layerId, int partitionId, int partitionCount)
        {
            using (var reader = _repository.ExecuteReaderSql(string.Format(GET_MODELLED_LAYER_YELT_QUERY_BY_PARTITION, lossAnalysisId, layerId, partitionCount, partitionId)))
            {
                while (reader.Read())
                {
                    yield return new RevoLayerYeltEntry
                    {
                        Year = (short)reader.GetInt32(0),
                        EventId = reader.GetInt32(1),
                        PerilId = (byte)Enum.Parse<RevoPeril>(reader.GetString(2)),
                        Day = reader.GetInt16(3),
                        LossPct = reader.GetDouble(4),
                        RP = reader.GetDouble(5),
                        RB = reader.GetDouble(6),
                    };
                }
            }
        }


        #endregion Methods

        #region Constants

 //       private const string GET_MODELLED_LAYER_YELT_QUERY = @"SELECT Year 
 //    , CAST(EventId AS INT) EventId
 //    , Peril
 //    , Day
 //    , SUM(LossPct) AS LossPct
 //    , SUM(RP) AS RP
 //    , SUM(RB) AS RB
 // FROM dbo.LayerYelt
 //WHERE LossAnalysisId = {0}
 //  AND LayerId  = {1}
 //  AND LossType = 1
 //GROUP BY Year 
 //    , CAST(EventId AS INT)
 //    , Peril
 //    , Day";

        private const string GET_MODELLED_LAYER_YELT_QUERY = @"SELECT Year 
     , CAST(EventId AS INT) EventId
     , Peril
     , Day
     , LossPct
     , RP
     , RB
  FROM dbo.LayerYelt
 WHERE LossAnalysisId = {0}
   AND LayerId  = {1}
   AND LossType = 1";

        private const string GET_MODELLED_LAYER_YELT_QUERY_BY_PARTITION = @"SELECT Year 
     , CAST(EventId AS INT) EventId
     , Peril
     , Day
     , LossPct
     , RP
     , RB
  FROM dbo.LayerYelt
 WHERE LossAnalysisId = {0}
   AND LayerId  = {1}
   AND LossType = 1
   AND (Year % {2}) = {3}";

        private const string GET_MODELLED_LAYER_YELT_QUERY_NO_RB = @"SELECT Year 
     , CAST(EventId AS INT) EventId
     , Peril
     , Day
     , LossPct
     , RP
  FROM dbo.LayerYelt
 WHERE LossAnalysisId = {0}
   AND LayerId  = {1}
   AND LossType = 1";

        private const string GET_MODELLED_LAYER_YELT_QUERY_NO_RP_NO_RB = @"SELECT Year 
     , CAST(EventId AS INT) EventId
     , Peril
     , Day
     , LossPct
  FROM dbo.LayerYelt
 WHERE LossAnalysisId = {0}
   AND LayerId  = {1}
   AND LossType = 1";

        #endregion Constants
    }
}
