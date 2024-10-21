using LargeLoss.LayeringService.Client;

namespace Arch.ILS.EconomicModel.Historical
{
    public class ScenarioLayerLossAggregateHeader
    {
        public ScenarioLayerLossAggregateHeader(string lossAggregateHeaderName, DateTime scenarioStartDate, DateTime scenarioEndDate, DateTime creationDate) 
        {
            LossAggregateHeaderName = lossAggregateHeaderName;
            ScenarioStartDate = scenarioStartDate.ToUniversalTime();
            ScenarioEndDate = scenarioEndDate.ToUniversalTime();
            CreationDate = creationDate.ToUniversalTime();
        }

        public int LossAggregateHeaderId { get; set; }
        public string LossAggregateHeaderName { get; }
        public DateTime ScenarioStartDate { get; }
        public DateTime ScenarioEndDate { get; }
        public DateTime CreationDate { get; }
    }
}
