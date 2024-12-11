
using Studio.Core;

namespace Arch.ILS.EconomicModel
{
    public class LayerLossAnalysisExtended : LayerLossAnalysis
    {
        public const double DefaultLoad = 1.0;

        public int? GUAnalysisId { get; set; }
        public double PerilEQ { get; set; }
        public double PerilWS { get; set; }
        public double PerilCS { get; set; }
        public double PerilWT { get; set; }
        public double PerilFL { get; set; }
        public double PerilWF { get; set; }
        public double GrowthEQ { get; set; }
        public double GrowthWS { get; set; }
        public double GrowthCS { get; set; }
        public double GrowthWT { get; set; }
        public double GrowthFL { get; set; }
        public double GrowthWF { get; set; }
        public double LAEEQ { get; set; }
        public double LAEWS { get; set; }
        public double LAECS { get; set; }
        public double LAEWT { get; set; }
        public double LAEFL { get; set; }
        public double LAEWF { get; set; }
        public double Inflation { get; set; }
        public double SocialEQ { get; set; }
        public double SocialWS { get; set; }
        public double SocialCS { get; set; }
        public double SocialWT { get; set; }
        public double SocialFL { get; set; }
        public double SocialWF { get; set; }
        public double CedentEQ { get; set; }
        public double CedentWS { get; set; }
        public double CedentCS { get; set; }
        public double CedentWT { get; set; }
        public double CedentFL { get; set; }
        public double CedentWF { get; set; }

        public double TotalLoadEQ => (1 + PerilEQ) * (1 + GrowthEQ) * (1 + LAEEQ) * (1 + SocialEQ) * (1 + CedentEQ) * (1 + Inflation);
        public double TotalLoadWS => (1 + PerilWS) * (1 + GrowthWS) * (1 + LAEWS) * (1 + SocialWS) * (1 + CedentWS) * (1 + Inflation);
        public double TotalLoadCS => (1 + PerilCS) * (1 + GrowthCS) * (1 + LAECS) * (1 + SocialCS) * (1 + CedentCS) * (1 + Inflation);
        public double TotalLoadWT => (1 + PerilWT) * (1 + GrowthWT) * (1 + LAEWT) * (1 + SocialWT) * (1 + CedentWT) * (1 + Inflation);
        public double TotalLoadFL => (1 + PerilFL) * (1 + GrowthFL) * (1 + LAEFL) * (1 + SocialFL) * (1 + CedentFL) * (1 + Inflation);
        public double TotalLoadWF => (1 + PerilWF) * (1 + GrowthWF) * (1 + LAEWF) * (1 + SocialWF) * (1 + CedentWF) * (1 + Inflation);

        public double TotalLoadxLAEEQ => (1 + PerilEQ) * (1 + GrowthEQ) * (1 + SocialEQ) * (1 + CedentEQ) * (1 + Inflation);
        public double TotalLoadxLAEWS => (1 + PerilWS) * (1 + GrowthWS) * (1 + SocialWS) * (1 + CedentWS) * (1 + Inflation);
        public double TotalLoadxLAECS => (1 + PerilCS) * (1 + GrowthCS) * (1 + SocialCS) * (1 + CedentCS) * (1 + Inflation);
        public double TotalLoadxLAEWT => (1 + PerilWT) * (1 + GrowthWT) * (1 + SocialWT) * (1 + CedentWT) * (1 + Inflation);
        public double TotalLoadxLAEFL => (1 + PerilFL) * (1 + GrowthFL) * (1 + SocialFL) * (1 + CedentFL) * (1 + Inflation);
        public double TotalLoadxLAEWF => (1 + PerilWF) * (1 + GrowthWF) * (1 + SocialWF) * (1 + CedentWF) * (1 + Inflation);

        public double LoadLAEEQ => 1 + LAEEQ;
        public double LoadLAEWS => 1 + LAEWS;
        public double LoadLAECS => 1 + LAECS;
        public double LoadLAEWT => 1 + LAEWT;
        public double LoadLAEFL => 1 + LAEFL;
        public double LoadLAEWF => 1 + LAEWF;

        public double GetTotalLoad(RevoPeril peril)
        {
            return peril switch
            {
                RevoPeril.EQ => TotalLoadEQ,
                RevoPeril.WS => TotalLoadWS,
                RevoPeril.CS => TotalLoadCS,
                RevoPeril.WT => TotalLoadWT,
                RevoPeril.FL => TotalLoadFL,
                RevoPeril.WF => TotalLoadWF,
                _ => DefaultLoad
            };
        }

        public double GetTotalLoadxLAE(RevoPeril peril)
        {
            return peril switch
            {
                RevoPeril.EQ => TotalLoadxLAEEQ,
                RevoPeril.WS => TotalLoadxLAEWS,
                RevoPeril.CS => TotalLoadxLAECS,
                RevoPeril.WT => TotalLoadxLAEWT,
                RevoPeril.FL => TotalLoadxLAEFL,
                RevoPeril.WF => TotalLoadxLAEWF,
                _ => DefaultLoad
            };
        }

        public double GetLAELoad(RevoPeril peril)
        {
            return peril switch
            {
                RevoPeril.EQ => LoadLAEEQ,
                RevoPeril.WS => LoadLAEWS,
                RevoPeril.CS => LoadLAECS,
                RevoPeril.WT => LoadLAEWT,
                RevoPeril.FL => LoadLAEFL,
                RevoPeril.WF => LoadLAEWF,
                _ => DefaultLoad
            };
        }
    }
}
