
using Studio.Core;

namespace Arch.ILS.EconomicModel
{
    public class RetroZone : IRecord
    {        
        //public int RetroZoneId { get; set; }
        [Field(0)]
        public int RetroProgramId { get; set; }
        //public string Name { get; set; }
        //public decimal ELLowerBound { get; set; }
        //public decimal ELUpperBound { get; set; }
        //public decimal ROLLowerBound { get; set; }
        //public decimal ROLUpperBound { get; set; }
        [Field(1)]
        public decimal Cession { get; set; }
        //public double CessionCap { get; set; }
        //public DateTime CreateDate { get; set; }
        //public string CreateUser { get; set; }
        //public DateTime ModifyDate { get; set; }
        //public string ModifyUser { get; set; }
        //public bool IsActive { get; set; }
        //public bool IsDeleted { get; set; }
        //public TimeSpan RowVersion { get; set; }
        //public double CessionCapAdjusted { get; set; }
        [Field(2)]
        public int TopUpZoneId { get; set; }
        //public DateTime StartDate { get; set; }
    }
}
