using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Linq;
using System.Reflection.Metadata;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace SBD_Proj2
{
    internal class IndexFileHandler : FileHandler
    {
        public long DiskPagesNumber;
        public long RootDiskPageIndex { get; set; } // Always 0, unless there is no root page - then NaN


        public IndexFileHandler(string filename)
            : base(filename)
        {
            DiskPagesNumber = 0;
            RootDiskPageIndex = Glob.NaN;
        }


        public bool InsertRecEntry(long newRecKey, long newRecIndex, long newRecLeftChildPageIdx, long newRecRightChildPageIdx)
        {
            if (GetRecIdx(newRecKey) != Glob.NaN)
                return false;

            IndexFileBuffer indexFileBuffer = (IndexFileBuffer)AssignedBuffer!;

            while (true)
            {
                if (DiskPageIdx == Glob.NaN) // Target page does not exist. It has to be a new root
                {
                    RootDiskPageIndex = DiskPagesNumber;
                    DiskPagesNumber++;
                    DiskPageIdx = RootDiskPageIndex;
                    indexFileBuffer.Clear();
                }

                if (indexFileBuffer.KeysCount < 2 * Glob.BTreeDegree)
                {
                    RegularlyInsertRecEntry(newRecKey, newRecIndex, newRecLeftChildPageIdx, newRecRightChildPageIdx);
                    return true;
                }
                else
                {
                    long originalDiskPageIdx = DiskPageIdx;
                    if (newRecLeftChildPageIdx != Glob.NaN)
                    {
                        ChangeDiskPage(newRecLeftChildPageIdx, false);
                        indexFileBuffer.ParentPageIndex = originalDiskPageIdx;
                        ChangeDiskPage(originalDiskPageIdx, true);
                    }
                    if (newRecRightChildPageIdx != Glob.NaN)
                    {
                        ChangeDiskPage(newRecRightChildPageIdx, false);
                        indexFileBuffer.ParentPageIndex = originalDiskPageIdx;
                        ChangeDiskPage(originalDiskPageIdx, true);
                    }

                    int leftSiblingKeysCount = 0;
                    long leftSiblingPageIdx = Glob.NaN;
                    int rightSiblingKeysCount = 0;
                    long rightSiblingPageIdx = Glob.NaN;

                    if (indexFileBuffer.ParentPageIndex != -1)
                        GetSiblingsKeysCount(ref leftSiblingKeysCount, ref rightSiblingKeysCount, ref leftSiblingPageIdx, ref rightSiblingPageIdx);

                    if (leftSiblingKeysCount > 0 && leftSiblingKeysCount < 2 * Glob.BTreeDegree)
                    {
                        MakeCompensation(newRecKey, newRecIndex, newRecLeftChildPageIdx, newRecRightChildPageIdx, true, leftSiblingPageIdx, leftSiblingKeysCount);
                        return true;
                    }
                    else if (rightSiblingKeysCount > 0 && rightSiblingKeysCount < 2 * Glob.BTreeDegree)
                    {
                        MakeCompensation(newRecKey, newRecIndex, newRecLeftChildPageIdx, newRecRightChildPageIdx, false, rightSiblingPageIdx, rightSiblingKeysCount);
                        return true;
                    }
                    else
                        MakeSplit(ref newRecKey, ref newRecIndex, ref newRecLeftChildPageIdx, ref newRecRightChildPageIdx);
                }
            }
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


        private void RegularlyInsertRecEntry(long newRecordKey, long newRecordIndex, long newRecLeftChildPageIdx, long newRecRightChildPageIdx)
        {
            IndexFileBuffer indexFileBuffer = (IndexFileBuffer)AssignedBuffer!;

            indexFileBuffer.InsertRecEntry(newRecordKey, newRecordIndex, newRecLeftChildPageIdx, newRecRightChildPageIdx);
            WriteBufferToFile();

            UpdateOneChildPageParent(newRecLeftChildPageIdx);
            UpdateOneChildPageParent(newRecRightChildPageIdx);
        }

        private void MakeCompensation(long newRecKey,long newRecIndex, long newRecLeftChildPageIdx, long newRecRightChildPageIdx,
            bool shiftLeft, long siblingPageIdx, int siblingKeysCount)
        {
            IndexFileBuffer buffer = (IndexFileBuffer)AssignedBuffer!;
            long originalPageIdx = DiskPageIdx;
            long parentPageIdx = buffer.ParentPageIndex;

            int allKeysCount = 2 * Glob.BTreeDegree + siblingKeysCount + 2; // 2: one in parent, one new
            int siblingNewKeysCount = allKeysCount - (allKeysCount / 2 + 1); // half of the keys stay on the original page

            ChangeDiskPage(parentPageIdx, true);
            int originalPageIdxIdx = buffer.GetChildPageIdxIdx(originalPageIdx); // index of the original page index in its parent's content

            if (shiftLeft)
                MakeCompensationLeft(new RecEntry(newRecKey, newRecIndex, newRecLeftChildPageIdx, newRecRightChildPageIdx),
                    originalPageIdxIdx, originalPageIdx, parentPageIdx, siblingPageIdx, allKeysCount, siblingKeysCount, siblingNewKeysCount);
            else
                MakeCompensationRight(new RecEntry(newRecKey, newRecIndex, newRecLeftChildPageIdx, newRecRightChildPageIdx),
                    originalPageIdxIdx, originalPageIdx, parentPageIdx, siblingPageIdx, allKeysCount, siblingKeysCount, siblingNewKeysCount);
        }

        private void MakeSplit(ref long newRecKey, ref long newRecIndex, ref long newRecLeftChildPageIdx, ref long newRecRightChildPageIdx)
        {
            IndexFileBuffer originalBuffer = (IndexFileBuffer)AssignedBuffer!;
            long originalDiskPageIdx = DiskPageIdx;

            // New buffer operations

            IndexFileBuffer newBuffer = new();
            AssignedBuffer = newBuffer;
            DiskPageIdx = DiskPagesNumber;
            DiskPagesNumber++;
            long newDiskPageIdx = DiskPageIdx;

            bool newRecEntryInserted = false;
            int curRecEntryIdx = MoveHalfRecEntriesToNewBuffer(originalBuffer, newRecKey, newRecIndex,
                newRecLeftChildPageIdx, newRecRightChildPageIdx, ref newRecEntryInserted);

            long movedHigherRecordKey = originalBuffer.GetRecKeyValue(curRecEntryIdx);
            long movedHigherRecordIndex = originalBuffer.GetRecIdxValue(curRecEntryIdx);
            curRecEntryIdx++;
            if (newRecKey < movedHigherRecordKey && !newRecEntryInserted) // new rec moved higher
            {
                movedHigherRecordKey = newRecKey;
                movedHigherRecordIndex = newRecIndex;
                curRecEntryIdx--;

                newBuffer.SetChildPageIdxValue(newBuffer.KeysCount, newRecLeftChildPageIdx);
            }

            WriteBufferToFile();
            UpdateAllChildPagesParent();

            // Original buffer operations

            AssignedBuffer = originalBuffer;
            DiskPageIdx = originalDiskPageIdx;

            OrganiseRestRecEntriesInOriginalBuffer(curRecEntryIdx);
            if (!newRecEntryInserted && newRecKey != movedHigherRecordKey)
            {
                originalBuffer.InsertRecEntry(newRecKey, newRecIndex, newRecLeftChildPageIdx, newRecRightChildPageIdx);
                WriteBufferToFile();

                UpdateOneChildPageParent(newRecLeftChildPageIdx);
                UpdateOneChildPageParent(newRecRightChildPageIdx);
            }
            else
                WriteBufferToFile();

            // Update the information about the record to be inserted next
            newRecKey = movedHigherRecordKey;
            newRecIndex = movedHigherRecordIndex;
            ChangeDiskPage(originalBuffer.ParentPageIndex, false);
            newRecLeftChildPageIdx = newDiskPageIdx;
            newRecRightChildPageIdx = originalDiskPageIdx;
        }


        public long GetRecIdx(long key)
        {
            long targetDiskPageIndex = RootDiskPageIndex;
            if (targetDiskPageIndex == Glob.NaN)
                return Glob.NaN;
            ChangeDiskPage(targetDiskPageIndex, false);

            IndexFileBuffer buffer = (IndexFileBuffer)AssignedBuffer!;

            while (true)
            {
                int i;
                for (i = 1; i <= buffer.KeysCount; i++)
                {
                    if (key < buffer.GetRecKeyValue(i))
                    {
                        targetDiskPageIndex = buffer.GetChildPageIdxValue(i - 1);
                        break;
                    }
                    if (key == buffer.GetRecKeyValue(i))
                        return buffer.GetRecIdxValue(i);
                }

                if (i == buffer.KeysCount + 1)
                    // key bigger than eny existinig key - go to the page pointed by the last page index
                    targetDiskPageIndex = buffer.GetChildPageIdxValue(i - 1);

                if (targetDiskPageIndex == Glob.NaN)
                {
                    if (key == Glob.NegInf)
                        return buffer.GetRecIdxValue(1);
                    else
                        return Glob.NaN;
                }

                ChangeDiskPage(targetDiskPageIndex, false);
            }
        }

        public long GetNextRecIdx(long curRecIdx)
        {
            IndexFileBuffer buffer = (IndexFileBuffer)AssignedBuffer!;
            int curRecEntryIdx = buffer.GetRecEntryIdxByRecIdx(curRecIdx);
            long rightChildPageIdx = buffer.GetChildPageIdxValue(curRecEntryIdx);

            if (rightChildPageIdx != Glob.NaN) // next rec entry is lower
            {
                ChangeDiskPage(rightChildPageIdx, false);
                while (buffer.GetChildPageIdxValue(0) != Glob.NaN)
                {
                    ChangeDiskPage(buffer.GetChildPageIdxValue(0), false);
                }
                curRecEntryIdx = 1;
            }
            else if (curRecEntryIdx < buffer.KeysCount) // next rec entry is the next entry on the same page
            {
                curRecEntryIdx++;
            }
            else if (buffer.ParentPageIndex != Glob.NaN) // next rec entry is higher/there is no next rec entry
            {
                long childDiskPageIdx = DiskPageIdx;
                ChangeDiskPage(buffer.ParentPageIndex, false);
                int childPageIdxIdx = buffer.GetChildPageIdxIdx(childDiskPageIdx);

                while (childPageIdxIdx == buffer.KeysCount)
                {
                    if (buffer.ParentPageIndex == Glob.NaN) // there is no next entry
                        return Glob.NaN;

                    childDiskPageIdx = DiskPageIdx;
                    ChangeDiskPage(buffer.ParentPageIndex, false);
                    childPageIdxIdx = buffer.GetChildPageIdxIdx(childDiskPageIdx);
                }
                curRecEntryIdx = childPageIdxIdx + 1;
            }
            else // there is no next entry
                return Glob.NaN;

            return buffer.GetRecIdxValue(curRecEntryIdx);
        }

        private void GetSiblingsKeysCount(ref int leftSiblingKeysCount, ref int rightSiblingKeysCount, ref long leftSiblingPageIdx, ref long rightSiblingPageIdx)
        {
            IndexFileBuffer buffer = (IndexFileBuffer)AssignedBuffer!;
            long originalPageIdx = DiskPageIdx;
            long parentPageIdx = buffer.ParentPageIndex;

            ChangeDiskPage(buffer.ParentPageIndex, false);
            int parentBufferKeysCount = buffer.KeysCount;

            int originalPageIdxIdx = buffer.GetChildPageIdxIdx(originalPageIdx); // index of the original page index in its parent's content
            int leftSiblingPageIdxIdx = originalPageIdxIdx - 1;
            int rightSiblingPageIdxIdx = originalPageIdxIdx + 1;

            if (leftSiblingPageIdxIdx >= 0 && leftSiblingPageIdxIdx <= buffer.KeysCount)
            {
                leftSiblingPageIdx = buffer.GetChildPageIdxValue(leftSiblingPageIdxIdx);
                ChangeDiskPage(leftSiblingPageIdx, false);
                leftSiblingKeysCount = buffer.KeysCount;
                ChangeDiskPage(parentPageIdx, false);
            }
            else
                leftSiblingKeysCount = 0;

            if (rightSiblingPageIdxIdx >= 0 && rightSiblingPageIdxIdx <= buffer.KeysCount)
            {
                rightSiblingPageIdx = buffer.GetChildPageIdxValue(rightSiblingPageIdxIdx);
                ChangeDiskPage(rightSiblingPageIdx, false);
                rightSiblingKeysCount = buffer.KeysCount;
            }
            else
                rightSiblingKeysCount = 0;

            ChangeDiskPage(originalPageIdx, false);
        }

        private void MakeCompensationLeft(RecEntry newRecEntry, int originalPageIdxIdx, long originalPageIdx,
            long parentPageIdx, long siblingPageIdx, int allKeysCount, int siblingKeysCount, int siblingNewKeysCount)
        {
            IndexFileBuffer buffer = (IndexFileBuffer)AssignedBuffer!;

            int recEntryInParentIdx = originalPageIdxIdx;
            long  recKeyInParent = buffer.GetRecKeyValue(recEntryInParentIdx);
            long recIdxInParent = buffer.GetRecIdxValue(recEntryInParentIdx);
            long leftChildPageIdxInParent = buffer.GetChildPageIdxValue(recEntryInParentIdx - 1);
            long rightChildPageIdxInParent = originalPageIdx;

            ChangeDiskPage(originalPageIdx, false);

            RecEntry recEntryInParent = new(recKeyInParent, recIdxInParent, leftChildPageIdxInParent, rightChildPageIdxInParent);
            bool newRecAdded = false;
            List<RecEntry> recEntriesToInsertToSibling = GetRecEntriesToInsertToLeftSibling(newRecEntry, recEntryInParent,
                allKeysCount, siblingKeysCount, ref newRecAdded);
            RecEntry newRecEntryInParent = GetNewRecEntryInParentDuringCompensationLeft(ref newRecAdded, newRecEntry, recEntriesToInsertToSibling);

            if (!newRecAdded)
            {
                buffer.InsertRecEntry(newRecEntry.Key, newRecEntry.Idx, newRecEntry.LeftChildPageIdx, newRecEntry.RightChildPageIdx);
                WriteBufferToFile();
                UpdateOneChildPageParent(newRecEntry.LeftChildPageIdx);
            }

            ChangeDiskPage(siblingPageIdx, true);
            foreach (RecEntry recEntry in recEntriesToInsertToSibling)
            {
                buffer.InsertRecEntry(recEntry.Key, recEntry.Idx, recEntry.LeftChildPageIdx, recEntry.RightChildPageIdx);
            }
            long curDiskPageIdx = DiskPageIdx;
            foreach (RecEntry recEntry in recEntriesToInsertToSibling)
            {
                ChangeDiskPage(recEntry.RightChildPageIdx, true);
                buffer.ParentPageIndex = curDiskPageIdx;
            }

            ChangeDiskPage(parentPageIdx, true);
            buffer.SetRecKeyValue(recEntryInParentIdx, newRecEntryInParent.Key);
            buffer.SetRecIdxValue(recEntryInParentIdx, newRecEntryInParent.Idx);
            WriteBufferToFile();
        }

        private void MakeCompensationRight(RecEntry newRecEntry, int originalPageIdxIdx, long originalPageIdx,
            long parentPageIdx, long siblingPageIdx, int allKeysCount, int siblingKeysCount, int siblingNewKeysCount)
        {
            IndexFileBuffer buffer = (IndexFileBuffer)AssignedBuffer!;
            
            int recEntryInParentIdx = originalPageIdxIdx + 1;
            long recKeyInParent = buffer.GetRecKeyValue(recEntryInParentIdx);
            long recIdxInParent = buffer.GetRecIdxValue(recEntryInParentIdx);
            long leftChildPageIdxInParent = originalPageIdx;
            long rightChildPageIdxInParent = buffer.GetChildPageIdxValue(recEntryInParentIdx);

            ChangeDiskPage(originalPageIdx, false);
            
            RecEntry recEntryInParent = new(recKeyInParent, recIdxInParent, leftChildPageIdxInParent, rightChildPageIdxInParent);
            bool newRecAdded = false;
            List<RecEntry> recEntriesToInsertToSibling = GetRecEntriesToInsertToRightSibling(newRecEntry, recEntryInParent,
                allKeysCount, siblingKeysCount, ref newRecAdded);
            bool newRecAddedToSibling = newRecAdded;

            RecEntry newRecEntryInParent = GetNewRecEntryInParentDuringCompensationRight(ref newRecAdded, newRecEntry, recEntriesToInsertToSibling);

            if (!newRecAdded)
            {
                buffer.InsertRecEntry(newRecEntry.Key, newRecEntry.Idx, newRecEntry.LeftChildPageIdx, newRecEntry.RightChildPageIdx);
                WriteBufferToFile();
                UpdateOneChildPageParent(newRecEntry.LeftChildPageIdx);
            }
            if (!newRecAddedToSibling && newRecAdded) // new rec added to the parent
            {
                buffer.SetChildPageIdxValue(buffer.KeysCount, newRecEntry.LeftChildPageIdx);
                WriteBufferToFile();
                UpdateOneChildPageParent(newRecEntry.LeftChildPageIdx);
            }

            ChangeDiskPage(siblingPageIdx, true);
            foreach (RecEntry recEntry in recEntriesToInsertToSibling)
            {
                buffer.InsertRecEntry(recEntry.Key, recEntry.Idx, recEntry.LeftChildPageIdx, recEntry.RightChildPageIdx);
            }
            long curDiskPageIdx = DiskPageIdx;
            foreach (RecEntry recEntry in recEntriesToInsertToSibling)
            {
                ChangeDiskPage(recEntry.LeftChildPageIdx, true);
                buffer.ParentPageIndex = curDiskPageIdx;
            }

            ChangeDiskPage(parentPageIdx, true);
            buffer.SetRecKeyValue(recEntryInParentIdx, newRecEntryInParent.Key);
            buffer.SetRecIdxValue(recEntryInParentIdx, newRecEntryInParent.Idx);
            WriteBufferToFile();
        }

        private void UpdateAllChildPagesParent()
        {
            IndexFileBuffer buffer = (IndexFileBuffer)AssignedBuffer!;

            for (int i = 0; i <= buffer.KeysCount; i++)
            {
                long childPageIdx = buffer.GetChildPageIdxValue(i);
                UpdateOneChildPageParent(childPageIdx);
            }
        }


        protected override void PrintPage(long diskPageIndex)
        {
            IndexFileBuffer buffer = (IndexFileBuffer)AssignedBuffer!;

            Console.ForegroundColor = ConsoleColor.DarkCyan;
            Console.Write("\n[INDEX FILE]");
            Console.WriteLine("[page: " + diskPageIndex + "]");
            Console.ForegroundColor = ConsoleColor.Cyan;

            Console.Write("[keys count: " + buffer.KeysCount + "]");
            Console.WriteLine("[parent page: " + buffer.ParentPageIndex + "]");

            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.WriteLine("[child page: " + buffer.GetChildPageIdxValue(0) + "]");
            Console.ForegroundColor = ConsoleColor.Gray;

            for (int i = 1; i <= 2 * Glob.BTreeDegree; i++)
            {
                Console.Write("[key: " + buffer.GetRecKeyValue(i) + "]");
                Console.WriteLine("[record: " + buffer.GetRecIdxValue(i) + "]");
                Console.ForegroundColor = ConsoleColor.DarkGray;
                Console.WriteLine("[child page: " + buffer.GetChildPageIdxValue(i) + "]");
                Console.ForegroundColor = ConsoleColor.Gray;
            }
        }

        private List<RecEntry> GetRecEntriesToInsertToLeftSibling(RecEntry newRecEntry, RecEntry recEntryinParent, int allKeysCount, int siblingKeysCount, ref bool newRecAdded)
        {
            IndexFileBuffer buffer = (IndexFileBuffer)AssignedBuffer!;

            List<RecEntry> recEntriesToInsertToSibling = new() // it is a form of a temporary buffer
            {
                new RecEntry(recEntryinParent.Key, recEntryinParent.Idx, Glob.Void, Glob.Void)
            };
            // not transferred: which stay + which is currently in parent + which goes to the parent + which are already
            int fromOriginalToSiblingCount = allKeysCount - (allKeysCount / 2 + 1 + 1 + siblingKeysCount);

            for (int i = 1; i <= fromOriginalToSiblingCount; i++)
            {
                long recKey = buffer.GetRecKeyValue(1);
                long recIdx = buffer.GetRecIdxValue(1);
                long leftChildPageIdx = buffer.GetChildPageIdxValue(0);

                if (newRecEntry.Key < recKey && !newRecAdded)
                {
                    recEntriesToInsertToSibling.Add(new RecEntry(newRecEntry.Key, newRecEntry.Idx, Glob.Void, Glob.Void));
                    recEntriesToInsertToSibling.ElementAt(i - 1).RightChildPageIdx = newRecEntry.LeftChildPageIdx;

                    newRecAdded = true;
                    fromOriginalToSiblingCount--;
                }
                else
                {
                    recEntriesToInsertToSibling.Add(new RecEntry(recKey, recIdx, Glob.Void, Glob.Void));
                    recEntriesToInsertToSibling.ElementAt(i - 1).RightChildPageIdx = leftChildPageIdx;

                    buffer.MovePossibleRecEntries(2, 1);
                    buffer.SetRecEntryAtPosition(0, 0, Glob.Void, 0, buffer.KeysCount);
                    buffer.KeysCount--;
                }
            }

            return recEntriesToInsertToSibling;
        }
        
        private List<RecEntry> GetRecEntriesToInsertToRightSibling(RecEntry newRecEntry, RecEntry recEntryinParent, int allKeysCount, int siblingKeysCount, ref bool newRecAdded)
        {
            IndexFileBuffer buffer = (IndexFileBuffer)AssignedBuffer!;

            List<RecEntry> recEntriesToInsertToSibling = new() // it is a form of a temporary buffer
            {
                new RecEntry(recEntryinParent.Key, recEntryinParent.Idx, Glob.Void, Glob.Void)
            };
            // not transferred: which stay + which is currently in parent + which goes to the parent + which are already
            int fromOriginalToSiblingCount = allKeysCount - (allKeysCount / 2 + 1 + 1 + siblingKeysCount);

            for (int i = 2 * Glob.BTreeDegree; i > 2 * Glob.BTreeDegree - fromOriginalToSiblingCount; i--)
            {
                long recKey = buffer.GetRecKeyValue(buffer.KeysCount);
                long recIdx = buffer.GetRecIdxValue(buffer.KeysCount);
                long rightChildPageIdx = buffer.GetChildPageIdxValue(buffer.KeysCount);

                if (newRecEntry.Key > recKey && !newRecAdded)
                {
                    // using LeftChildPageIdx != Void because if there is a new page then it is its index
                    recEntriesToInsertToSibling.Add(new RecEntry(newRecEntry.Key, newRecEntry.Idx, newRecEntry.LeftChildPageIdx, Glob.Void));
                    recEntriesToInsertToSibling.ElementAt(recEntriesToInsertToSibling.Count - 2).LeftChildPageIdx = newRecEntry.RightChildPageIdx;

                    newRecAdded = true;
                    fromOriginalToSiblingCount--;
                }
                else
                {
                    recEntriesToInsertToSibling.Add(new RecEntry(recKey, recIdx, Glob.Void, Glob.Void));
                    if (recEntriesToInsertToSibling.ElementAt(recEntriesToInsertToSibling.Count - 2).LeftChildPageIdx != newRecEntry.LeftChildPageIdx)
                        recEntriesToInsertToSibling.ElementAt(recEntriesToInsertToSibling.Count - 2).LeftChildPageIdx = rightChildPageIdx;

                    buffer.SetRecEntryAtPosition(0, 0, Glob.Void, 0, buffer.KeysCount);
                    buffer.KeysCount--;
                }
            }

            return recEntriesToInsertToSibling;
        }

        private RecEntry GetNewRecEntryInParentDuringCompensationLeft(ref bool newRecAdded, RecEntry newRecEntry, List<RecEntry> recEntriesToInsertToSibling)
        {
            IndexFileBuffer buffer = (IndexFileBuffer)AssignedBuffer!;
            RecEntry newRecEntryInParent;

            long originalBufferFirstRecKey = buffer.GetRecKeyValue(1);
            long originalBufferFirstRecIdx = buffer.GetRecIdxValue(1);
            long originalBufferFirstLeftChildPageIdx = buffer.GetChildPageIdxValue(0);

            if (!newRecAdded && newRecEntry.Key < originalBufferFirstRecKey)
            {
                newRecEntryInParent = new RecEntry(newRecEntry.Key, newRecEntry.Idx, Glob.Void, Glob.Void);
                recEntriesToInsertToSibling.ElementAt(recEntriesToInsertToSibling.Count - 1).RightChildPageIdx = newRecEntry.LeftChildPageIdx;

                newRecAdded = true;
            }
            else
            {
                newRecEntryInParent = new RecEntry(originalBufferFirstRecKey, originalBufferFirstRecIdx, Glob.Void, Glob.Void);
                recEntriesToInsertToSibling.ElementAt(recEntriesToInsertToSibling.Count - 1).RightChildPageIdx = originalBufferFirstLeftChildPageIdx;

                buffer.MovePossibleRecEntries(2, 1);
                buffer.SetRecEntryAtPosition(0, 0, Glob.Void, 0, buffer.KeysCount);
                buffer.KeysCount--;
            }

            return newRecEntryInParent;
        }

        private RecEntry GetNewRecEntryInParentDuringCompensationRight(ref bool newRecAdded, RecEntry newRecEntry, List<RecEntry> recEntriesToInsertToSibling)
        {
            IndexFileBuffer buffer = (IndexFileBuffer)AssignedBuffer!;
            RecEntry newRecEntryInParent;

            long originalBufferLastRecKey = buffer.GetRecKeyValue(buffer.KeysCount);
            long originalBufferLastRecIdx = buffer.GetRecIdxValue(buffer.KeysCount);
            long originalBufferLastRightChildPageIdx = buffer.GetChildPageIdxValue(buffer.KeysCount);

            if (!newRecAdded && newRecEntry.Key > originalBufferLastRecKey)
            {
                newRecEntryInParent = new RecEntry(newRecEntry.Key, newRecEntry.Idx, Glob.Void, Glob.Void);
                recEntriesToInsertToSibling.ElementAt(recEntriesToInsertToSibling.Count - 1).LeftChildPageIdx = newRecEntry.RightChildPageIdx;

                newRecAdded = true;
            }
            else
            {
                newRecEntryInParent = new RecEntry(originalBufferLastRecKey, originalBufferLastRecIdx, Glob.Void, Glob.Void);
                if (recEntriesToInsertToSibling.ElementAt(recEntriesToInsertToSibling.Count - 1).LeftChildPageIdx != newRecEntry.LeftChildPageIdx)
                    recEntriesToInsertToSibling.ElementAt(recEntriesToInsertToSibling.Count - 1).LeftChildPageIdx = originalBufferLastRightChildPageIdx;

                buffer.SetRecEntryAtPosition(0, 0, Glob.Void, 0, buffer.KeysCount);
                buffer.KeysCount--;
            }

            return newRecEntryInParent;
        }

        private int MoveHalfRecEntriesToNewBuffer(IndexFileBuffer originalBuffer, long newRecKey, long newRecordIndex, long newRecLeftChildPageIdx, long newRecRightChildPageIdx, ref bool newRecEntryInserted)
        {
            IndexFileBuffer newBuffer = (IndexFileBuffer)AssignedBuffer!;
            int curRecEntryIdx;
            int fromOriginalToNew = Glob.BTreeDegree;

            for (curRecEntryIdx = 1; curRecEntryIdx <= fromOriginalToNew; curRecEntryIdx++) // iterate through half of the record entries
            {
                long recKey = originalBuffer.GetRecKeyValue(curRecEntryIdx);
                long recIndex = originalBuffer.GetRecIdxValue(curRecEntryIdx);
                long recLeftChildPageIdx = originalBuffer.GetChildPageIdxValue(curRecEntryIdx - 1);
                long recRightChildPageIdx = originalBuffer.GetChildPageIdxValue(curRecEntryIdx);

                if ((recKey < newRecKey && !newRecEntryInserted) || (newRecEntryInserted))
                {
                    newBuffer.InsertRecEntry(recKey, recIndex, recLeftChildPageIdx, recRightChildPageIdx);
                }
                else
                {
                    newBuffer.InsertRecEntry(newRecKey, newRecordIndex, newRecLeftChildPageIdx, newRecRightChildPageIdx);
                    newRecEntryInserted = true;
                    curRecEntryIdx--; // read the existing record entry once again
                    fromOriginalToNew--;
                }
            }

            return curRecEntryIdx;
        }

        private void OrganiseRestRecEntriesInOriginalBuffer(int firstRemainingRecEntryIdx)
        {
            IndexFileBuffer originalBuffer = (IndexFileBuffer)AssignedBuffer!;
            int newKeysCount = originalBuffer.KeysCount - (firstRemainingRecEntryIdx - 1);
            
            originalBuffer.MovePossibleRecEntries(firstRemainingRecEntryIdx, 1);

            // Clear rest of the entries spaces
            for (int i = newKeysCount + 1; i <= originalBuffer.KeysCount; i++)
            {
                originalBuffer.SetRecKeyValue(i, 0);
                originalBuffer.SetRecIdxValue(i, 0);
                originalBuffer.SetChildPageIdxValue(i, 0);
            }
            originalBuffer.KeysCount = newKeysCount;
        }

        private void UpdateOneChildPageParent(long childPageIdx)
        {
            long curDiskPageIdx = DiskPageIdx;

            if (childPageIdx != Glob.NaN)
            {
                ChangeDiskPage(childPageIdx, false);
                ((IndexFileBuffer)AssignedBuffer!).ParentPageIndex = curDiskPageIdx;
                ChangeDiskPage(curDiskPageIdx, true);
            }
            // else: allowed behaviour
        }
    }
}
