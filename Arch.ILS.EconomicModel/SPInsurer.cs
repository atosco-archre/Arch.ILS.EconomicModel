using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Arch.ILS.EconomicModel
{
    public class SPInsurer
    {
        public int SPInsurerId { get; set; }
        public int RetroProgramId { get; set; }
        //public string SegregatedAccount { get; set; }
        //public string ContractId { get; set; }
        //public int InsurerId { get; set; }
        //public string TrustBank { get; set; }
        //public DateTime CreateDate { get; set; }
        //public string CreateUser { get; set; }
        //public DateTime ModifyDate { get; set; }
        //public string ModifyUser { get; set; }
        //public bool IsActive { get; set; }
        //public bool IsDeleted { get; set; }
        public long RowVersion { get; set; }
        //public string TrustAccountNumber { get; set; }
        //public string FundsWithheldAccountNumber { get; set; }
        public DateTime? InitialCommutationDate { get; set; }
        public DateTime? FinalCommutationDate { get; set; }
    }
}
