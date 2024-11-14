using LargeLoss.LayeringService.Client;

namespace Arch.ILS.EconomicModel.Historical
{
    public class ScenarioLayerLossAggregate
    {
        public ScenarioLayerLossAggregate(DtoLayeringOutput dtoLayeringOutput)
        {
            ScenarioId = long.Parse(dtoLayeringOutput.EventId);
            EventDate = dtoLayeringOutput.EventDate.Value.DateTime.ToUniversalTime().Date;
            LayerId = (int)dtoLayeringOutput.LayerId;
            LayerInceptionDate = dtoLayeringOutput.InceptionDate.Value.DateTime.ToUniversalTime().Date;
            LayerExpirationDate = dtoLayeringOutput.ExpirationDate.Value.DateTime.ToUniversalTime().Date;
            SimulationInceptionDate = dtoLayeringOutput.SimulationInceptionDate.Value.DateTime.ToUniversalTime().Date;
            SimulationExpirationDate = dtoLayeringOutput.SimulationExpirationDate.Value.DateTime.ToUniversalTime().Date;
            SimulationUWYear = (int)dtoLayeringOutput.SimulationYear;
            IsFHCF = (bool)dtoLayeringOutput.IsFHCF;
            LAE = dtoLayeringOutput.Lae.Value;
            LossCurrency = dtoLayeringOutput.Currency;
            GULoss = dtoLayeringOutput.GuLoss.Value;
            SectionsAdjustment = dtoLayeringOutput.SectionsAdjustment.Value;
            OccLoss100Pct = dtoLayeringOutput.OccLoss100Pct.Value;
            OccLoss = dtoLayeringOutput.OccLoss.Value;
            LayerLoss100Pct = dtoLayeringOutput.LayerLoss100Pct.Value;
            LayerLoss = dtoLayeringOutput.LayerLoss.Value;
            AggLoss100Pct = dtoLayeringOutput.AggLoss100Pct.Value;
            AggLoss = dtoLayeringOutput.AggLoss.Value;
            LastCumulativeOccLoss100Pct = dtoLayeringOutput.LastCumulativeOccLoss100Pct.Value;
            LastAggLoss100Pct = dtoLayeringOutput.LastAggLoss100Pct.Value;
            NewAggLimit = dtoLayeringOutput.NewAggLimit.Value;
            NewAggRetention = dtoLayeringOutput.NewAggRetention.Value;
            ReinstatementPremium100Pct = dtoLayeringOutput.ReinstatementPremium100Pct ?? 0.0m;
            ReinstatementPremium = dtoLayeringOutput.ReinstatementPremium ?? 0.0m;
            ReinstatementBrokerage100Pct = dtoLayeringOutput.ReinstatementBrokerage100Pct ?? 0.0m;
            ReinstatementBrokerage = dtoLayeringOutput.ReinstatementBrokerage ?? 0.0m;
            Placement = dtoLayeringOutput.Placement.Value;
        }

        public int LossAggregateHeaderId { get; set; }
        public long ScenarioId { get; }
        public DateTime EventDate { get; }
        public long LayerId { get; }
        public DateTime LayerInceptionDate { get; }
        public DateTime LayerExpirationDate { get; }
        public DateTime SimulationInceptionDate { get; }
        public DateTime SimulationExpirationDate { get; }
        public int SimulationUWYear { get; }
        public bool IsFHCF { get; }
        public decimal LAE { get; }
        public string LossCurrency { get; }
        public decimal GULoss { get; }
        public decimal SectionsAdjustment { get; }
        public decimal OccLoss100Pct { get; }
        public decimal OccLoss { get; }
        public decimal LayerLoss100Pct { get; }
        public decimal LayerLoss { get; }
        public decimal AggLoss100Pct { get; }
        public decimal AggLoss { get; }
        public decimal LastCumulativeOccLoss100Pct { get; }
        public decimal LastAggLoss100Pct { get; }
        public decimal NewAggLimit { get; }
        public decimal NewAggRetention { get; }
        public decimal ReinstatementPremium100Pct { get; }
        public decimal ReinstatementPremium { get; }
        public decimal ReinstatementBrokerage100Pct { get; }
        public decimal ReinstatementBrokerage { get; }
        public decimal Placement { get; }
    }
}
