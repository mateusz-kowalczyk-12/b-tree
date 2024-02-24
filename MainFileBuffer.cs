using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SBD_Proj2
{
    // MainFileBuffer is a buffer that stores one main file disk page.
    // The structure of a main file disk page is a sequence of elements:
    // [RECORD_0 ... RECORD_(N) EMPTY_SPACE_0 ... EMPTY_SPACE_M].
    // Empty spaces are filled with the byte '\0'

    [Serializable]
    internal class MainFileBuffer : Buffer
    {
        public MainFileBuffer()
        {
            Array.Resize(
                ref _content,
                Glob.RecordSpacesPerMainFileDiskPage * Glob.BytesPerRecord
            );
        }
    }
}
