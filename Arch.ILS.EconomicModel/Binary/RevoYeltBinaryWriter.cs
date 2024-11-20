
using System.Runtime.InteropServices;

namespace Arch.ILS.EconomicModel.Binary
{
    public class RevoYeltBinaryWriter
    {
        public const int CURRENT_BINARY_WRITER_VERSION = 0;//v0
        public const int CURRENT_BINARY_WRITER_HEADER_LENGTH = 26;//v1

        public const int BINARY_WRITER_VERSION_INDEX = 0;
        public const int LOSSANALYSISID_INDEX = 4;
        public const int LAYERID_INDEX = 8;
        public const int ROWVERSION_INDEX = 12;
        public const int TOTALENTRYCOUNT_INDEX = 20;
        public const int HASRP_INDEX = 24;
        public const int HASRB_INDEX = 25;

        public const int BINARY_WRITER_VERSION_SIZE = 4;
        public const int LOSSANALYSISID_SIZE = 4;
        public const int LAYERID_SIZE = 4;
        public const int ROWVERSION_SIZE = 8;
        public const int TOTALENTRYCOUNT_SIZE = 4;
        public const int HASRP_SIZE = 1;
        public const int HASRB_SIZE = 1;

        private readonly IYelt _yelt;

        public RevoYeltBinaryWriter(in IYelt yelt)
        {            
            _yelt = yelt;
        }

        public void WriteAll(in string filePath)
        {
            using (BinaryWriter writer = new BinaryWriter(new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None)))
            {
                writer.Write(CURRENT_BINARY_WRITER_VERSION);
                writer.Write(_yelt.LossAnalysisId);
                writer.Write(_yelt.LayerId);
                writer.Write(_yelt.RowVersion);
                writer.Write(_yelt.TotalEntryCount);
                writer.Write(_yelt.HasRP);
                writer.Write(_yelt.HasRB);

                for(uint i  = 0; i < _yelt.BufferCount; i++)
                    writer.Write(MemoryMarshal.Cast<long, byte>(_yelt.YearDayEventIdPerilIdKeys(i)));
                for (uint i = 0; i < _yelt.BufferCount; i++)
                    writer.Write(MemoryMarshal.Cast<short, byte>(_yelt.Days(i)));
                for (uint i = 0; i < _yelt.BufferCount; i++)
                    writer.Write(MemoryMarshal.Cast<double, byte>(_yelt.LossPcts(i)));
                if(_yelt.HasRP)
                {
                    for (uint i = 0; i < _yelt.BufferCount; i++)
                        writer.Write(MemoryMarshal.Cast<double, byte>(_yelt.RPs(i)));
                }
                if(_yelt.HasRB)
                {
                    for (uint i = 0; i < _yelt.BufferCount; i++)
                        writer.Write(MemoryMarshal.Cast<double, byte>(_yelt.RBs(i)));
                }
            }
        }
    }
}
