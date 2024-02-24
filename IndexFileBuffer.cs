using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SBD_Proj2
{
    // IndexFileBuffer is a buffer that stores one B-tree disk page.
    // The structure of a main file disk page is a sequence of elements:
    // [HEADER P_0 X_1 A_1 ... P_M X_M A_M P_M], where HEADER
    // is a sequence of elements: [KEYS_COUNT PARENT_PAGE_INDEX], P_i is
    // the index of a child page, X_i is the key and A_i is the index
    // of the record in the main file. Some of the above may be empty.
    // Empty spaces are filled with the byte '\0'

    [Serializable]
    internal class IndexFileBuffer : Buffer
    {
        public int KeysCount
        {
            get => BitConverter.ToInt32(Content, 0);
            set => BitConverter.GetBytes(value).CopyTo(Content, 0);
        }
        public long ParentPageIndex
        {
            get => BitConverter.ToInt64(Content, Glob.BytesPerIndexFileDiskPageKeysCount);
            set => BitConverter.GetBytes(value).CopyTo(Content, Glob.BytesPerIndexFileDiskPageKeysCount);
        }


        public IndexFileBuffer()
        {
            Array.Resize(
                ref _content,
                Glob.BytesPerIndexFileDiskPageHeader + Glob.BytesPerDiskPageIndex
                    + 2 * Glob.BTreeDegree * (Glob.BytesPerRecordKey + Glob.BytesPerRecordIndex + Glob.BytesPerDiskPageIndex)
            );

            ParentPageIndex = Glob.NaN;
            SetChildPageIdxValue(0, Glob.NaN);
            SetChildPageIdxValue(1, Glob.NaN);
        }

        /// <summary>
        /// Assumes there is enough space to insert the record
        /// </summary>
        public void InsertRecEntry(long recordKey, long recordIndex, long leftChildPageIdx, long rightChildPageIdx)
        {
            int spaceIdx = 1;

            if (KeysCount > 0)
                spaceIdx = MakeSpaceForNewRecEntry(recordKey);

            SetRecEntryAtPosition(recordKey, recordIndex, leftChildPageIdx, rightChildPageIdx, spaceIdx);
            KeysCount++;
        }


        public void SetRecEntryAtPosition(long recordKey, long recordIndex, long leftChildPageIdx, long rightChildPageIdx, int spaceIdx)
        {
            SetRecKeyValue(spaceIdx, recordKey);
            SetRecIdxValue(spaceIdx, recordIndex);

            if (leftChildPageIdx != Glob.Void)
                SetChildPageIdxValue(spaceIdx - 1, leftChildPageIdx);
            if (rightChildPageIdx != Glob.Void)
                SetChildPageIdxValue(spaceIdx, rightChildPageIdx);
        }

        public int MakeSpaceForNewRecEntry(long recordKey)
        {
            int spaceIndex;
            for (spaceIndex = 1; spaceIndex <= KeysCount; spaceIndex++)
            {
                if (GetRecKeyValue(spaceIndex) > recordKey)
                {
                    MovePossibleRecEntries(spaceIndex, spaceIndex + 1);
                    break;
                }
            }

            return spaceIndex;
        }


        public void MovePossibleRecEntries(int startEntryIdx, int targetStartEntryIdx)
        {
            if (targetStartEntryIdx < startEntryIdx) // move to the left
            {
                for (int j = startEntryIdx; j <= KeysCount; j++)
                {
                    MoveRecEntry(j, j + (targetStartEntryIdx - startEntryIdx));
                }
            }
            else // move to the right
            {
                for (int j = KeysCount; j >= startEntryIdx; j--)
                {
                    MoveRecEntry(j, j + targetStartEntryIdx - startEntryIdx);
                }
            }
        }


        public override void Clear()
        {
            for (int i = 0; i < Content.Length; i++)
            {
                Content[i] = 0;
            }

            ParentPageIndex = Glob.NaN;
            SetChildPageIdxValue(0, Glob.NaN);
            SetChildPageIdxValue(1, Glob.NaN);
        }

        public void MoveRecEntry(int entryIdx, int targetEntryIdx)
        {
            SetRecIdxValue(targetEntryIdx, GetRecIdxValue(entryIdx));
            SetRecKeyValue(targetEntryIdx, GetRecKeyValue(entryIdx));

            if (entryIdx < targetEntryIdx)
            {
                SetChildPageIdxValue(targetEntryIdx, GetChildPageIdxValue(entryIdx));
                SetChildPageIdxValue(targetEntryIdx - 1, GetChildPageIdxValue(entryIdx - 1));
            }
            else
            {
                SetChildPageIdxValue(targetEntryIdx - 1, GetChildPageIdxValue(entryIdx - 1));
                SetChildPageIdxValue(targetEntryIdx, GetChildPageIdxValue(entryIdx));
            }
        }

        public int GetChildPageIdxIdx(long childPageIdxValue)
        {
            for (int i = 0; i <= KeysCount; i++)
            {
                if (GetChildPageIdxValue(i) == childPageIdxValue)
                    return i;
            }

            return (int)Glob.NaN;
        }

        public int GetRecKeyIdx(long recKey)
        {
            for (int i = 0; i <= KeysCount; i++)
            {
                if (GetRecKeyValue(i) == recKey)
                    return i;
            }

            return (int)Glob.NaN;
        }

        public int GetRecEntryIdxByRecIdx(long recIdx)
        {
            for (int i = 1; i <= KeysCount; i++)
            {
                if (GetRecIdxValue(i) == recIdx)
                    return i;
            }

            return (int)Glob.NaN;
        }


        public void SetChildPageIdxValue(int index, long value)
        {
            BitConverter.GetBytes(value)
                .CopyTo(
                    Content, 
                    Glob.BytesPerIndexFileDiskPageHeader
                        + index * Glob.BytesPerIndexFileDiskPageRecordEntry
                );
        }

        public long GetChildPageIdxValue(int index)
        {
            return BitConverter.ToInt64(
                Content,
                Glob.BytesPerIndexFileDiskPageHeader
                    + index * Glob.BytesPerIndexFileDiskPageRecordEntry
            );
        }

        public void SetRecKeyValue(int index, long value)
        {
            BitConverter.GetBytes(value)
                .CopyTo(
                    Content,
                    Glob.BytesPerIndexFileDiskPageHeader + Glob.BytesPerDiskPageIndex
                        + (index - 1) * Glob.BytesPerIndexFileDiskPageRecordEntry
                );
        }

        public long GetRecKeyValue(int index)
        {
            return BitConverter.ToInt64(
                Content,
                Glob.BytesPerIndexFileDiskPageHeader + Glob.BytesPerDiskPageIndex
                    + (index - 1) * Glob.BytesPerIndexFileDiskPageRecordEntry
            );
        }

        public void SetRecIdxValue(int index, long value)
        {
            BitConverter.GetBytes(value)
                .CopyTo(
                    Content,
                    Glob.BytesPerIndexFileDiskPageHeader + Glob.BytesPerDiskPageIndex + Glob.BytesPerRecordKey
                        + (index - 1) * Glob.BytesPerIndexFileDiskPageRecordEntry
                );
        }

        public long GetRecIdxValue(int index)
        {
            return BitConverter.ToInt64(
                Content,
                Glob.BytesPerIndexFileDiskPageHeader + Glob.BytesPerDiskPageIndex + Glob.BytesPerRecordKey
                    + (index - 1) * Glob.BytesPerIndexFileDiskPageRecordEntry
            );
        }
    }
}
