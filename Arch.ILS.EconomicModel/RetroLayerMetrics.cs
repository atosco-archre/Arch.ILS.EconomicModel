
namespace Arch.ILS.EconomicModel
{
    public record class RetroLayerMetrics(in byte RetroLevel, in int RetroProgramid, in RetroProgramType RetroProgramType, in DateTime RetroInceptionDate, in DateTime RetroExpirationDate, 
        in int LayerId, in DateTime LayerInceptionDate, in DateTime LayerExpirationDate, ContractStatus LayerStatus, in DateTime StartDateInclusive, in DateTime EndDateInclusive, in decimal RetroPlacement, in decimal DepositPremium,
        in decimal SubjectPremium, in decimal SubjectPremiumPlaced, in decimal CededPremium, 
        in decimal DepositLimit, in decimal SubjectLimit, in decimal SubjectLimitPlaced, in decimal CededLimit,
        in decimal GrossCessionAfterPlacement, in decimal NetCession);
}
