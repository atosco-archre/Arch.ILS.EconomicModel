
using System.ComponentModel;

namespace Arch.ILS.EconomicModel
{
    public enum LayerType : short
    {
        [Description("")]
        Undefined = 0,

        [Description("Cascade")]
        Cascade = 1,

        [Description("Top drop")]
        Top_drop = 2,

        [Description("2nd Event")]
        Second_Event = 3,

        [Description("2nd/3rd Event")]
        Second_Third_Event = 4,

        [Description("FHCF Mirror")]
        FHCF_Mirror = 5,

        [Description("Quote option")]
        Quote_option = 6,

        [Description("Shortfall")]
        Shortfall = 7,

        [Description("Stub Cover")]
        Stub_Cover = 8,

        [Description("3rd/4th Event")]
        Third_Fourth_Event = 9,

        [Description("Wind")]
        Wind = 10,

        [Description("Quake")]
        Quake = 11,

        [Description("Combined")]
        Combined = 12,

        [Description("2 Year")]
        Two_Year = 13,

        [Description("3 Year")]
        Three_Year = 14,

        [Description("RPP")]
        RPP = 15,

        [Description("Top & Agg")]
        Top_and_Agg = 16,

        [Description("Agg XoL")]
        Agg_XoL = 17,

        [Description("Stop Loss")]
        Stop_Loss = 18,

        [Description("Subject Layer")]
        Subject_Layer = 19,

        [Description("FHCF")]
        FHCF = 20,

        [Description("Agg ILW")]
        Agg_ILW = 21,

        [Description("CAT Quota Share")]
        CAT_Quota_Share = 22,

        [Description("CAT & Risk")]
        CAT_and_Risk = 23,

        [Description("CAT XoL")]
        CAT_XoL = 24,

        [Description("Combined-JP")]
        Combined_JP = 25,

        [Description("ILW")]
        ILW = 26,

        [Description("Non Cat Quota Share")]
        Non_Cat_Quota_Share = 27,

        [Description("Per Risk")]
        Per_Risk = 28,

        [Description("Quake-JP")]
        Quake_JP = 29,

        [Description("Risk Agg")]
        Risk_Agg = 30,

        [Description("Risk Occ Event")]
        Risk_Occ_Event = 31,

        [Description("Umbrella")]
        Umbrella = 32,

        [Description("Wind-JP")]
        Wind_JP = 33,

        [Description("Agri Non Prop")]
        Agri_Non_Prop = 36,

        [Description("Agri Prop")]
        Agri_Prop = 38,

        [Description("Annuity Cover")]
        Annuity_Cover = 40,

        [Description("Aquaculture Fac QS")]
        Aquaculture_Fac_QS = 42,

        [Description("Aquaculture Non Prop")]
        Aquaculture_Non_Prop = 44,

        [Description("Aquaculture Prop")]
        Aquaculture_Prop = 46,

        [Description("Bond Fac Quota Share")]
        Bond_Fac_Quota_Share = 48,

        [Description("Bond Fac XoL")]
        Bond_Fac_XoL = 50,

        [Description("Bond Quota Share")]
        Bond_Quota_Share = 52,

        [Description("Bond Stop Loss")]
        Bond_Stop_Loss = 54,

        [Description("Bond XoL")]
        Bond_XoL = 56,

        [Description("Casualty Quota Share")]
        Casualty_Quota_Share = 58,

        [Description("Casualty Stop Loss")]
        Casualty_Stop_Loss = 60,

        [Description("Casualty XoL")]
        Casualty_XoL = 62,

        [Description("Casualty XoL Unlimited")]
        Casualty_XoL_Unlimited = 64,

        [Description("Credit Fac Quota Share")]
        Credit_Fac_Quota_Share = 66,

        [Description("Credit Fac XoL")]
        Credit_Fac_XoL = 68,

        [Description("Credit Quota Share")]
        Credit_Quota_Share = 70,

        [Description("Credit Stop Loss")]
        Credit_Stop_Loss = 72,

        [Description("Credit XoL")]
        Credit_XoL = 74,

        [Description("Crop Fac")]
        Crop_Fac = 76,

        [Description("Crop Quota Share")]
        Crop_Quota_Share = 78,

        [Description("Crop Stop Loss")]
        Crop_Stop_Loss = 80,

        [Description("Cyber")]
        Cyber = 82,

        [Description("Engineering XoL")]
        Engineering_XoL = 84,

        [Description("Fac QS")]
        Fac_QS = 86,

        [Description("Fac XOL")]
        Fac_XOL = 88,

        [Description("Forestry Fac QS")]
        Forestry_Fac_QS = 90,

        [Description("Forestry Non Prop")]
        Forestry_Non_Prop = 92,

        [Description("Forestry Prop")]
        Forestry_Prop = 94,

        [Description("Hail Quota Share")]
        Hail_Quota_Share = 96,

        [Description("Hail Stop Loss")]
        Hail_Stop_Loss = 98,

        [Description("Livestock Non Prop")]
        Livestock_Non_Prop = 100,

        [Description("Livestock Quota Share")]
        Livestock_Quota_Share = 102,

        [Description("Motor Quota Share")]
        Motor_Quota_Share = 104,

        [Description("Motor Stop Loss")]
        Motor_Stop_Loss = 106,

        [Description("Motor XoL")]
        Motor_XoL = 108,

        [Description("Motor XoL Unlimited")]
        Motor_XoL_Unlimited = 110,

        [Description("Multiline Quota Share")]
        Multiline_Quota_Share = 112,

        [Description("Multiline XoL")]
        Multiline_XoL = 114,

        [Description("Per Risk XoL")]
        Per_Risk_XoL = 116,

        [Description("Quota Share")]
        Quota_Share = 118,

        [Description("Quota Share and Surplus")]
        Quota_Share_and_Surplus = 120,

        [Description("Risk and Cat XoL")]
        Risk_and_Cat_XoL = 122,

        [Description("Risk XoL")]
        Risk_XoL = 124,

        [Description("Surplus")]
        Surplus = 126,

        [Description("Wild Fire")]
        Wild_Fire = 128,

        [Description("FAC")]
        FAC = 130,

        [Description("Subsequent Event")]
        Subsequent_Event = 132,

        [Description("Common Account")]
        Common_Account = 134,

        [Description("WIL")]
        WIL = 136,

        [Description("Agg WIL")]
        Agg_WIL = 138,

        [Description("Whole Account")]
        Whole_Account = 140,

        [Description("Cat on D&F")]
        Cat_on_D_and_F = 142,

        [Description("Surety Bond Attaching Variable QS")]
        Surety_Bond_Attaching_Variable_QS = 144,

        [Description("Surety XOL")]
        Surety_XOL = 146,

        [Description("Surety Bonds Attaching QS")]
        Surety_Bonds_Attaching_QS = 148,

        [Description("Casualty Primary")]
        Casualty_Primary = 150,

        [Description("Casualty Agg")]
        Casualty_Agg = 152,

        [Description("Multiline Casualty")]
        Multiline_Casualty = 154,

        [Description("Clash XoL")]
        Clash_XoL = 156,

        [Description("Casualty Working XS")]
        Casualty_Working_XS = 158,

        [Description("Occurrence XOL")]
        Occurrence_XOL = 160,

        [Description("Parametric")]
        Parametric = 162,
    }
}
