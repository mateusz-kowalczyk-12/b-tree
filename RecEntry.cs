using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SBD_Proj2
{
    internal class RecEntry
    {
        public long Key { get; set; }
        public long Idx { get; set; }
        public long LeftChildPageIdx { get; set; }
        public long RightChildPageIdx { get; set; }


        public RecEntry(long key, long idx, long leftChildPageIdx, long rightChildPageIdx)
        {
            Key = key;
            Idx = idx;
            LeftChildPageIdx = leftChildPageIdx;
            RightChildPageIdx = rightChildPageIdx;
        }
    }
}
