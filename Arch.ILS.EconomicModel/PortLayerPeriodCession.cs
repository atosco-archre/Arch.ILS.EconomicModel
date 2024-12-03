
namespace Arch.ILS.EconomicModel
{
    public readonly struct PortLayerPeriodCession
    {
        public PortLayerPeriodCession(in byte retroLevel, in int retroProgramId, in int portLayerId, in decimal grossCession, ref readonly PeriodCession periodCession) 
        {
            RetroLevel = retroLevel;
            RetroProgramId = retroProgramId;
            PortLayerId = portLayerId;
            GrossCession = grossCession;
            PeriodCession = periodCession;
        }

        public byte RetroLevel { get; init; }
        public int RetroProgramId { get; init; }
        public int PortLayerId { get; init; }
        public decimal GrossCession { get; init; }
        public PeriodCession PeriodCession { get; init; }
    }
}
