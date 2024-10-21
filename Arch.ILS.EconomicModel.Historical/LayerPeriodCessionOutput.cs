
namespace Arch.ILS.EconomicModel.Historical
{
    public class LayerPeriodCessionOutput
    {
        public LayerPeriodCessionOutput(ref LayerPeriodCession layerPeriodCession) 
        {
            RetroLevel = layerPeriodCession.RetroLevel;
            RetroProgramId = layerPeriodCession.RetroProgramId;
            LayerId = layerPeriodCession.LayerId;
            StartInclusive = layerPeriodCession.PeriodCession.StartInclusive;
            EndInclusive = layerPeriodCession.PeriodCession.EndInclusive;
            NetCession = layerPeriodCession.PeriodCession.NetCession;
        }

        public int LayerPeriodCessionHeaderId { get; set; }
        public byte RetroLevel { get; }
        public long RetroProgramId { get; }
        public long LayerId { get; }
        public DateTime StartInclusive { get;  }
        public DateTime EndInclusive { get;  }
        public decimal NetCession { get; }
    }
}
