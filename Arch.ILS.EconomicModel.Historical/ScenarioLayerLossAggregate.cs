using LargeLoss.LayeringService.Client;

namespace Arch.ILS.EconomicModel.Historical
{
    public class ScenarioLayerLossAggregate
    {
        public ScenarioLayerLossAggregate(DtoLayeringOutput dtoLayeringOutput) 
        {
            ScenarioId = long.Parse(dtoLayeringOutput.EventId);
            EventDate = dtoLayeringOutput.EventDate.Value.DateTime.ToUniversalTime();
            LayerId = (int)dtoLayeringOutput.LayerId;
            LayerInceptionDate = dtoLayeringOutput.InceptionDate.Value.DateTime.ToUniversalTime();
            LayerExpirationDate = dtoLayeringOutput.ExpirationDate.Value.DateTime.ToUniversalTime();
            SimulationInceptionDate = dtoLayeringOutput.SimulationInceptionDate.Value.DateTime.ToUniversalTime();
            SimulationExpirationDate = dtoLayeringOutput.SimulationExpirationDate.Value.DateTime.ToUniversalTime();
            SimulationUWYear = (int)dtoLayeringOutput.SimulationYear;
            IsFHCF = (bool)dtoLayeringOutput.IsFHCF;
            LAE = (decimal)dtoLayeringOutput.Lae.Value;
            LossCurrency = dtoLayeringOutput.Currency;
            GULoss = (decimal)dtoLayeringOutput.GuLoss.Value;
            LayerLoss = (decimal)dtoLayeringOutput.LayerLoss.Value;
            SectionsAdjustment = (decimal)dtoLayeringOutput.SectionsAdjustment.Value;
            LastCumulativeOccLoss = (decimal)dtoLayeringOutput.LastCumulativeOccLoss.Value;
            LastAggLoss = (decimal)dtoLayeringOutput.LastAggLoss.Value;
            OccLoss = (decimal)dtoLayeringOutput.OccLoss.Value;
            AggLoss = (decimal)dtoLayeringOutput.AggLoss.Value;
            NewAggLimit = (decimal)dtoLayeringOutput.NewAggLimit.Value;
            NewAggRetention = (decimal)dtoLayeringOutput.NewAggRetention.Value;
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
        public decimal LayerLoss { get; }
        public decimal SectionsAdjustment { get; }
        public decimal LastCumulativeOccLoss { get; }
        public decimal LastAggLoss { get; }
        public decimal OccLoss { get; }
        public decimal AggLoss { get; }
        public decimal NewAggLimit { get; }
        public decimal NewAggRetention { get; }
    }
}
