
namespace Arch.ILS.EconomicModel
{
    public class FXRate
    {
        public int FXRateId { get; set; }
        public string BaseCurrency { get; set; }
        public string Currency { get; set; }
        public DateTime FXDate { get; set; }
        public decimal Rate { get; set; }
        //public DateTime CreateDate { get; set; }
        //public string CreateUser { get; set; }
        //public DateTime ModifyDate { get; set; }
        //public string ModifyUser { get; set; }
        //public bool IsActive { get; set; }
        //public bool IsDeleted { get; set; }
        public long RowVersion { get; set; }
    }
}
