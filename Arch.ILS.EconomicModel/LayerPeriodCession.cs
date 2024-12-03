
namespace Arch.ILS.EconomicModel
{
    public readonly struct LayerPeriodCession
    {
        public LayerPeriodCession(in byte retroLevel, in int retroProgramId, in int layerId, in decimal grossCession, ref readonly PeriodCession periodCession) 
        {
            RetroLevel = retroLevel;
            RetroProgramId = retroProgramId;
            LayerId = layerId;
            GrossCession = grossCession;
            PeriodCession = periodCession;
        }

        public byte RetroLevel { get; init; }
        public int RetroProgramId { get; init; }
        public int LayerId { get; init; }
        public decimal GrossCession { get; init; }
        public PeriodCession PeriodCession { get; init; }
    }
}
