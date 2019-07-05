using System;
using System.Collections.Generic;
using System.Linq;

namespace MsS_SQL
{
    class Program
    {
        private static object s;
        private static object o;

        public static void Main(string[] args)
        {
            var msOwner = "ADSPACE_MPHH";
            var oraOwner = "ADSPACE_MC";
            var connectionSQL = $"Data Source=ADV09;Initial Catalog={msOwner};User ID={msOwner};Password={msOwner}";
            var connectionOracle = $"Data Source=ADV02;Persist Security Info=True;User ID={oraOwner};Password={oraOwner};";

            var SQLrepository = new Repository(connectionSQL, DbType.MsSQL, msOwner);
            var ORArepository = new Repository(connectionOracle, DbType.Oracle, oraOwner);

            var listOfTableNamesSQL = SQLrepository.GetTableNames();
            var listOfTableNamesOracle = ORArepository.GetTableNames();

            var Diff = SQLrepository.FindDifferenceInTablesDict(listOfTableNamesSQL, listOfTableNamesOracle);
            var DiffList = SQLrepository.FindDifferenceInTablesList(listOfTableNamesSQL, listOfTableNamesOracle);

            var listOfTableAndColsNamesSQL = SQLrepository.GetListOfColsAndTables();
            var listOfTableAndColsNamesORA = ORArepository.GetListOfColsAndTables();

            var diffsInStructure = SQLrepository.FindDifferenceInCols(listOfTableAndColsNamesSQL, listOfTableAndColsNamesORA);

            Console.WriteLine("{0, 30}     |{1, 30}\n-------------------------------------------------------------------", "Oracle", "SQL");
            foreach (var item in DiffList)
            {
                if (listOfTableNamesOracle.Contains(item))
                {
                    Console.WriteLine("{0, 30}     |{1, 30}", item, "----------");
                }
                if (listOfTableNamesSQL.Contains(item))
                {
                    Console.WriteLine("{0, 30}     |{1, 30}", "----------", item);
                }
            }
            Console.WriteLine("\n--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                              "{0, 67}     ||{1, 60}" +
                              "\n--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n", "Oracle", "SQL");
            foreach (var item in diffsInStructure)
            {
                var objSQL = listOfTableAndColsNamesSQL.Find(x => x.Table == item.Key);
                var objORA = listOfTableAndColsNamesORA.Find(x => x.Table == item.Key);
                if (objORA!= null && objSQL != null)
                {
                    foreach(var i in item.Value)
                    {
                        if (objORA.Columns.Contains(i))
                        {
                            Console.WriteLine("{0, 30}     |{1, 30}      ||{2,30}     |{3,30}", item.Key, i, item.Key, "-----");
                        }
                        if (objSQL.Columns.Contains(i))
                        {
                            Console.WriteLine("{0, 30}     |{1, 30}      ||{2,30}     |{3,30}", item.Key, "-----", item.Key, i);
                        }
                    }
                }
                Console.WriteLine("--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
            }
            Console.ReadKey();
        }
    }
}
