using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Metadata.Ecma335;
using System.Text;
using System.Threading.Tasks;

namespace SBD_Proj2
{
    internal class Glob
    {
        public static long NaN { get; }
        public static long Void { get; }
        public static long NegInf { get; }

        // Record is an array containing a sequence of elements: [KEY ARRAY],
        // where ARRAY is a sequence of values: [VALUE_0 ... VALUE_N]
        public static int ValuesPerRecordArray { get; }
        public static int BytesPerRecordKey { get; }
        public static int BytesPerRecordArrayValue { get; }
        public static int BytesPerRecordArray { get => ValuesPerRecordArray * BytesPerRecordArrayValue; }
        public static int BytesPerRecord { get => BytesPerRecordKey + BytesPerRecordArray; }
        public static int RecordSpacesPerMainFileDiskPage { get; }

        public static int BytesPerDiskPageIndex { get; }
        public static int BytesPerRecordIndex { get; }
        public static int BytesPerIndexFileDiskPageRecordEntry { get => BytesPerRecordKey + BytesPerRecordIndex + BytesPerDiskPageIndex; }
        public static int BytesPerIndexFileDiskPageKeysCount { get; }
        public static int BytesPerIndexFileDiskPageHeader { get => BytesPerIndexFileDiskPageKeysCount + BytesPerDiskPageIndex; }
        public static int BTreeDegree { get; }

        public static int DiskReads { get; set; }
        public static int DiskWrites { get; set; }


        static Glob()
        {
            NaN = -1;
            Void = -2;
            NegInf = -3;

            ValuesPerRecordArray = 5;
            BytesPerRecordKey = sizeof(long);
            BytesPerRecordArrayValue = sizeof(int);
            RecordSpacesPerMainFileDiskPage = 4;

            BytesPerDiskPageIndex = sizeof(long);
            BytesPerRecordIndex = sizeof(long);
            BytesPerIndexFileDiskPageKeysCount = sizeof(int);
            BTreeDegree = 2;

            DiskReads = 0;
            DiskWrites = 0;
        }


        public static long GetRecordKey(byte[] record)
        {
            return BitConverter.ToInt64(record, 0);
        }
    }
}