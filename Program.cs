using SBD_Proj2;
using System.Runtime.Serialization.Formatters.Binary;


void Main()
{
    Dbms dbms = new();
    MainMenu(dbms);
}


void MainMenu(Dbms dbms)
{
    bool exit = false;
    bool menuOutput = true;
    bool recsNumberShowing = false;

    while (!exit)
    {
        if (menuOutput)
        {
            Console.Write(
                "\nMain menu:\n" +
                "0: Toggle menu output mode (set to: " + menuOutput + ")\n" +
                "1: Toggle records in the file number showing (set to: " + recsNumberShowing + ")\n" +
                "2: Create new file\n" +
                "3: Add records\n" +
                "4: Get record by key\n" +
                "5: Get all records (sorted)\n" +
                "6: Show main and index file content\n" +
                "7: Exit\n" +
                "> "
            );
        }

        string choice = Console.ReadLine()!;
        switch (choice[0])
        {
            case '0':
                menuOutput = !menuOutput;
                break;
            case '1':
                recsNumberShowing = !recsNumberShowing;
                break;
            case '2':
                CreateNewFileMenu(dbms, menuOutput);
                break;
            case '3':
                RecordAddingMenu(dbms, menuOutput, recsNumberShowing);
                break;
            case '4':
                RecordReadingByKeyMenu(dbms, menuOutput, recsNumberShowing);
                break;
            case '5':
                Glob.DiskReads = 0;
                Glob.DiskWrites = 0;
                PrintRecordsInFileNumber(dbms,  recsNumberShowing, menuOutput);

                dbms.ShowAllRecords();

                PrintDiskOperationsNumber(menuOutput);
                break;
            case '6':
                FilesContentShowingMenu(dbms, menuOutput, recsNumberShowing);
                break;
            case '7':
                exit = true;
                break;
            case '8':
                TestDataGeneratingMenu(menuOutput);
                break;
            default:
                break;
        }
    }
}


void CreateNewFileMenu(Dbms dbms, bool menuOutput)
{
    if (menuOutput)
    {
        Console.Write(
            "\nFile creating menu\n" +
            "- filename: "
        );
    }
    string filename = Console.ReadLine()!;

    dbms.CreateNewFile(filename);
}

void RecordAddingMenu(Dbms dbms, bool menuOutput, bool recsNumberShowing)
{
    if (menuOutput)
    {
        Console.Write(
        "\nRecords adding menu:\n" +
        "- records number: "
        );
    }
    int recordsNumber = Convert.ToInt32(Console.ReadLine()!);
    
    if (menuOutput)
        Console.WriteLine();

    for (int i = 0; i < recordsNumber; i++)
    {
        byte[] record = new byte[Glob.BytesPerRecord];
        long key = 0;
        int arrayValue;

        if (menuOutput)
            Console.Write("- record array values: ");

        string valuesString = Console.ReadLine()!;
        string[] valuesStringSplit = valuesString.Split(' ');

        for (int j = 0; j < Glob.ValuesPerRecordArray; j++)
        {
            arrayValue = Convert.ToInt32(valuesStringSplit[j]);
            key += arrayValue;
            BitConverter.GetBytes(arrayValue).CopyTo(
                record,
                Glob.BytesPerRecordKey + j * Glob.BytesPerRecordArrayValue
            );
        }

        BitConverter.GetBytes(key).CopyTo(record, 0);

        Glob.DiskReads = 0;
        Glob.DiskWrites = 0;
        PrintRecordsInFileNumber(dbms, recsNumberShowing, menuOutput);

        if (!dbms.SaveRecord(record, i == recordsNumber - 1))
            Console.WriteLine("[USER ERROR]: Record with this key is already present in the file!");

        PrintDiskOperationsNumber(menuOutput);
    }
}

void RecordReadingByKeyMenu(Dbms dbms, bool menuOutput, bool recsNumberShowing)
{
    if (menuOutput)
    {
        Console.Write(
            "\nRecord reading by index menu:\n" +
            "- key: "
        );
    }
    long recordIndex = Convert.ToInt64(Console.ReadLine()!);

    Glob.DiskReads = 0;
    Glob.DiskWrites = 0;

    byte[]? record = dbms.GetRecordByKey(recordIndex);

    if (record != null)
        PrintRecord(record);
    else
        Console.WriteLine("[USER ERROR]: No record with such key!");

    PrintRecordsInFileNumber(dbms, recsNumberShowing, menuOutput);
    PrintDiskOperationsNumber(menuOutput);
}

void FilesContentShowingMenu(Dbms dbms, bool menuOutput, bool recsNumberShowing)
{
    Glob.DiskReads = 0;
    Glob.DiskWrites = 0;

    dbms.ShowFilesContent();

    PrintRecordsInFileNumber(dbms, recsNumberShowing, menuOutput);
    PrintDiskOperationsNumber(menuOutput);
}

void TestDataGeneratingMenu(bool menuOutput)
{
    if (menuOutput)
    {
        Console.Write(
            "\nTest data generating menu:\n" +
            "- records number: "
        );
    }
    long recsN = Convert.ToInt64(Console.ReadLine()!);

    if (menuOutput)
        Console.Write("- max key value: ");
    long maxKeyValue = Convert.ToInt64(Console.ReadLine()!);

    if (menuOutput)
        Console.Write("- test file number: ");
    int testFileNumber = Convert.ToInt32(Console.ReadLine()!);

    FileStream testFile = File.Open("../../../tests/test" + testFileNumber + ".in", FileMode.Create);
    using StreamWriter writer = new(testFile);
    
    List<Int64> testKeys = new();
    long newKey;
    Random rand = new();

    writer.WriteLine("2");
    writer.WriteLine("file0");
    writer.WriteLine("3");
    writer.WriteLine(recsN);

    for (long i = 0; i < recsN; i++)
    {
        do
            newKey = rand.NextInt64() % (maxKeyValue - 4) + 5; // <5, max>
        while (testKeys.Contains(newKey));

        testKeys.Add(newKey);
        writer.WriteLine(newKey + " 0 0 0 0");
    }
}


void PrintRecord(byte[] record)
{
    Console.Write("[key: " + BitConverter.ToInt64(record, 0) + "] ");
    for (int i = 0; i < Glob.ValuesPerRecordArray; i++)
    {
        Console.Write(BitConverter.ToInt32(record, Glob.BytesPerRecordKey + i * Glob.BytesPerRecordArrayValue));
        if (i != Glob.ValuesPerRecordArray - 1)
            Console.Write(" ");
    }
    Console.WriteLine();
}

void PrintDiskOperationsNumber(bool menuOutput)
{
    if (menuOutput)
    {
        Console.ForegroundColor = ConsoleColor.DarkGray;
        Console.WriteLine("disk reads: " + Glob.DiskReads + "\ndisk writes: " + Glob.DiskWrites);
        Console.ResetColor();
    }
}

void PrintRecordsInFileNumber(Dbms dbms, bool recsNumberShowing, bool menuOutput)
{
    if (recsNumberShowing && menuOutput)
    {
        Console.ForegroundColor = ConsoleColor.DarkGray;
        Console.WriteLine("records in file: " + dbms.RecordsNumber);
        Console.ResetColor();
    }
}


Main();