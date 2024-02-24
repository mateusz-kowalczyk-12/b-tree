using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace SBD_Proj2
{
    internal abstract class FileHandler
    {
        public string Filename { get; set; }
        public string FilePath { get; set; }

        public Buffer? AssignedBuffer { get; set; }

        public long DiskPageIdx { get; set; }


        public FileHandler(string filename)
        {
            Filename = filename;
            FilePath = "../../../dbms_files/" + filename;

            AssignedBuffer = null;

            DiskPageIdx = Glob.NaN;
        }


        public void CreateFile()
        {
            using FileStream _ = File.Open(FilePath, FileMode.Create, FileAccess.Write);
        }


        public abstract void ShowFileContent();


        protected abstract void PrintPage(long diskPageIndex);

        public void WriteBufferToFile()
        {
            if (DiskPageIdx == Glob.NaN)
                return;

            while (true)
            {
                try
                {
                    using FileStream fStream = File.OpenWrite(FilePath);
                    using BinaryWriter binaryWriter = new(fStream);

                    fStream.Seek(DiskPageIdx * AssignedBuffer!.Content.Length, SeekOrigin.Begin);

                    binaryWriter.Write(AssignedBuffer!.Content);

                    Glob.DiskWrites++;
                    break;
                }
                catch (IOException)
                {
                    // The file lock has not been removed on time. Must wait
                    System.Threading.Thread.Sleep(100);
                }
            }
        }

        public bool ReadBufferFromFile()
        {
            if (DiskPageIdx == Glob.NaN) // Reading nothing. Allowed behaviour
                return false;

            while (true)
            {
                try
                {
                    using FileStream fStream = File.OpenRead(FilePath);
                    using BinaryReader binaryReader = new(fStream);

                    long fileLength = new FileInfo(FilePath).Length;
                    long seekIndex = DiskPageIdx * AssignedBuffer!.Content.Length;

                    if (seekIndex < fileLength)
                    {
                        fStream.Seek(seekIndex, SeekOrigin.Begin);
                        binaryReader.Read(AssignedBuffer!.Content, 0, AssignedBuffer!.Content.Length);

                        Glob.DiskReads++;
                        return true;
                    }
                    else // File end reached. Allowed behaviour
                    {
                        AssignedBuffer!.Clear();
                        return false;
                    }
                }
                catch (IOException)
                {
                    // The file lock has not been removed on time. Must wait
                    System.Threading.Thread.Sleep(100);
                }
            }
        }


        protected bool ChangeDiskPage(long targetDiskPageIndex, bool writeCurrent)
        {
            if (targetDiskPageIndex != DiskPageIdx)
            {
                if (DiskPageIdx != Glob.NaN && writeCurrent)
                    WriteBufferToFile();

                DiskPageIdx = targetDiskPageIndex;
                return ReadBufferFromFile();
            }
            return true;
        }
    }
}
