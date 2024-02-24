using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Metadata;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace SBD_Proj2
{
    internal class MainFileHandler : FileHandler
    {
        public MainFileHandler(string filename)
            : base(filename)
        {
        }


        public override void ShowFileContent()
        {
            long diskPageIndex = 0;

            while (ChangeDiskPage(diskPageIndex, false))
            {
                PrintPage(diskPageIndex);
                diskPageIndex++;
            }

            ChangeDiskPage(Glob.NaN, false);
        }


        protected override void PrintPage(long diskPageIndex)
        {
            Console.ForegroundColor = ConsoleColor.DarkGreen;
            Console.Write("\n[MAIN FILE]");
            Console.WriteLine("[page: " + diskPageIndex + "]");
            Console.ForegroundColor = ConsoleColor.Gray;

            for (int i = 0; i < Glob.RecordSpacesPerMainFileDiskPage; i++)
            {
                Console.Write("[record: " + (diskPageIndex * Glob.RecordSpacesPerMainFileDiskPage + i) + "]");
                Console.Write("[key: " + BitConverter.ToInt64(AssignedBuffer!.Content, i * Glob.BytesPerRecord) + "] ");

                for (int j = 0; j < Glob.ValuesPerRecordArray; j++)
                {
                    Console.Write(
                        BitConverter.ToInt32(
                            AssignedBuffer!.Content,
                            Glob.BytesPerRecordKey + i * Glob.BytesPerRecord + j * Glob.BytesPerRecordArrayValue
                        ) + " "
                    );
                }

                Console.WriteLine();
            }
        }

        public byte[] GetRecord(long recordIndex)
        {
            long targetDiskPageIndex = recordIndex / Glob.RecordSpacesPerMainFileDiskPage;
            ChangeDiskPage(targetDiskPageIndex, false);

            byte[] record = new byte[Glob.BytesPerRecord];
            Array.Copy(
                AssignedBuffer!.Content, (recordIndex % Glob.RecordSpacesPerMainFileDiskPage) * Glob.BytesPerRecord,
                record, 0,
                Glob.BytesPerRecord
            );

            return record;
        }

        public void SaveRecord(byte[] record, long recordIndex)
        {
            long targetDiskPageIndex = recordIndex / Glob.RecordSpacesPerMainFileDiskPage;
            ChangeDiskPage(targetDiskPageIndex, true);

            record.CopyTo(
                AssignedBuffer!.Content,
                (recordIndex % Glob.RecordSpacesPerMainFileDiskPage) * Glob.BytesPerRecord
            );
        }
    }
}
