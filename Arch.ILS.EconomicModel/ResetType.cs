
namespace Arch.ILS.EconomicModel
{
    public enum ResetType : byte
    {
        LOD = 1,/*Reset applying to all layers on the losses occurring after the reset date*/
        RAD = 2/*Reset applying to layers only attaching / incepting after the reset date*/
    }
}
