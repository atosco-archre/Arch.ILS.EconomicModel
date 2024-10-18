
namespace Arch.ILS.EconomicModel
{
    public class RetroProfile
    {
        public int RetroProfileId { get; set; }
        public string Name { get; set; }
        public string RegisId { get; set; }
        public int ManagerId { get; set; }
        public int CompanyId { get; set; }
        public int OfficeId { get; set; }
        public int DeptId { get; set; }
        //public DateTime CreateDate { get; set; }
        //public string CreateUser { get; set; }
        //public DateTime ModifyDate { get; set; }
        //public string ModifyUser { get; set; }
        //public bool IsActive { get; set; }
        //public bool IsDeleted { get; set; }
        public long RowVersion { get; set; }
    }
}
