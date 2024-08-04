
namespace Arch.ILS.EconomicModel
{
    public readonly struct PortLayerPeriodCession
    {
        public PortLayerPeriodCession(in byte retroLevel, in int retroProgramId, in int portLayerId, ref readonly PeriodCession periodCession) 
        {
            RetroLevel = retroLevel;
            RetroProgramId = retroProgramId;
            PortLayerId = portLayerId;
            PeriodCession = periodCession;
        }

        public byte RetroLevel { get; init; }
        public int RetroProgramId { get; init; }
        public int PortLayerId { get; init; }
        public PeriodCession PeriodCession { get; init; }
    }
}
