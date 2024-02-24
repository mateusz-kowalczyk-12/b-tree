using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SBD_Proj2
{
    struct FileHandlersPair
    {
        public MainFileHandler MainFileHandler { get; set; }
        public IndexFileHandler IndexFileHandler { get; set; }


        public FileHandlersPair(MainFileHandler mainFileHandler, IndexFileHandler indexFileHandler)
        {
            MainFileHandler = mainFileHandler;
            IndexFileHandler = indexFileHandler;
        }
    }


    internal class Dbms
    {
        private FileHandlersPair? FileHandlersPair { get; set; }

        private MainFileBuffer MainFileBuffer { get; }
        private Buffer IndexFileBuffer { get; }

        private long NewRecordIndex { get; set; }
        public long RecordsNumber { get; set; }

        private string StateFilePath { get; }


        public Dbms()
        {
            FileHandlersPair = null;

            MainFileBuffer = new MainFileBuffer();
            IndexFileBuffer = new IndexFileBuffer();

            NewRecordIndex = 0;
            RecordsNumber = 0;

            StateFilePath = "../../../dbms_files/.state";
        }


        public void CreateNewFile(string mainFileName)
        {
            CreateFileHandlersPair(mainFileName);

            ((FileHandlersPair)FileHandlersPair!).MainFileHandler.CreateFile();
            ((FileHandlersPair)FileHandlersPair!).IndexFileHandler.CreateFile();

            NewRecordIndex = 0;
            RecordsNumber = 0;
        }

        public void UseExistingFile(string mainFileName)
        {
            CreateFileHandlersPair(mainFileName);

            string[] stateLines = File.Exists(StateFilePath) ?
                File.ReadAllLines(StateFilePath) : Array.Empty<string>();

            string[] stateLineSplit = stateLines
                .Where(sl => sl.Split(' ')[0] == mainFileName)
                .First()
                .Split(' ');

            NewRecordIndex = Convert.ToInt64(stateLineSplit[1]);
            RecordsNumber = NewRecordIndex;

            IndexFileHandler indexFilehandler = ((FileHandlersPair)FileHandlersPair!).IndexFileHandler;
            indexFilehandler.RootDiskPageIndex = Convert.ToInt64(stateLineSplit[2]);
        }

        /// <summary>
        /// Saves the DBMS state in a file of the following format: <br/>
        /// [main file name (without ".main" extension)] [records number] [root index file disk page index]
        /// </summary>
        public void SaveState()
        {
            MainFileHandler mainFileHandler = ((FileHandlersPair)FileHandlersPair!).MainFileHandler;
            IndexFileHandler indexFileHandler = ((FileHandlersPair)FileHandlersPair!).IndexFileHandler;
            string mainFilePath = mainFileHandler.FilePath;
            string mainFileName = Path.GetFileNameWithoutExtension(mainFilePath);

            string[] stateLines = File.Exists(StateFilePath) ?
                File.ReadAllLines(StateFilePath) : Array.Empty<string>();
            bool found = false;

            stateLines = stateLines
                .Select(sl =>
                {
                    string[] split = sl.Split(' ');
                    if (split[0] == mainFileName)
                    {
                        found = true;
                        return mainFileName + " " + RecordsNumber + " " + indexFileHandler.RootDiskPageIndex;
                    }
                    return sl;
                })
                .ToArray();

            if (!found)
                stateLines = stateLines
                    .Append(mainFileName + " " + RecordsNumber + " " + indexFileHandler.RootDiskPageIndex)
                    .ToArray();

            File.WriteAllLines(StateFilePath, stateLines);
        }


        public bool SaveRecord(byte[] record, bool lastRecord)
        {
            MainFileHandler mainFileHandler = ((FileHandlersPair)FileHandlersPair!).MainFileHandler;
            IndexFileHandler indexFileHandler = ((FileHandlersPair)FileHandlersPair!).IndexFileHandler;

            if (indexFileHandler.InsertRecEntry(Glob.GetRecordKey(record), NewRecordIndex, Glob.NaN, Glob.NaN))
            {
                mainFileHandler.SaveRecord(record, NewRecordIndex);
                RecordsNumber++;
                NewRecordIndex++;

                if (lastRecord)
                    mainFileHandler.WriteBufferToFile();

                return true;
            }
            return false;
        }

        public byte[]? GetRecordByKey(long key)
        {
            IndexFileHandler indexFileHandler = ((FileHandlersPair)FileHandlersPair!).IndexFileHandler;
            long recordIndex = indexFileHandler.GetRecIdx(key);

            if (recordIndex != Glob.NaN)
                return GetRecordByIndex(recordIndex);
            return null;
        }

        public void ShowAllRecords()
        {
            MainFileHandler mainFileHandler = ((FileHandlersPair)FileHandlersPair!).MainFileHandler;
            IndexFileHandler indexFileHandler = ((FileHandlersPair)FileHandlersPair!).IndexFileHandler;

            long recIdx = indexFileHandler.GetRecIdx(Glob.NegInf);

            while (recIdx != Glob.NaN)
            {
                byte[] rec = mainFileHandler.GetRecord(recIdx);

                Console.Write("[key: " + Glob.GetRecordKey(rec) + "] ");

                for (int i = 0; i < Glob.ValuesPerRecordArray; i++)
                {
                    Console.Write(
                        BitConverter.ToInt32(
                            rec,
                            Glob.BytesPerRecordKey + i * Glob.BytesPerRecordArrayValue
                         ) + " "
                    );
                }
                Console.WriteLine();

                recIdx = indexFileHandler.GetNextRecIdx(recIdx);
            }
        }

        public void ShowFilesContent()
        {
            ((FileHandlersPair)FileHandlersPair!).MainFileHandler.ShowFileContent();
            ((FileHandlersPair)FileHandlersPair!).IndexFileHandler.ShowFileContent();
        }


        public byte[] GetRecordByIndex(long recordIndex)
        {
            MainFileHandler mainFileHandler = ((FileHandlersPair)FileHandlersPair!).MainFileHandler;
            return mainFileHandler.GetRecord(recordIndex);
        }


        private void CreateFileHandlersPair(string mainFileName)
        {
            MainFileHandler mainFileHandler = new(mainFileName + ".main");
            IndexFileHandler indexFileHandler = new(mainFileName + ".index");

            ResetBuffers();
            mainFileHandler.AssignedBuffer = MainFileBuffer;
            indexFileHandler.AssignedBuffer = IndexFileBuffer;

            FileHandlersPair = new FileHandlersPair(mainFileHandler, indexFileHandler);
        }

        private void ResetBuffers()
        {
            for (int i = 0; i < MainFileBuffer.Content.Length; i++)
                MainFileBuffer.Content[i] = 0;
            for (int i = 0; i < IndexFileBuffer.Content.Length; i++)
                IndexFileBuffer.Content[i] = 0;
        }
    }
}
