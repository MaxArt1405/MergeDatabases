using System;
using System.Collections.Generic;
using System.Linq;

namespace MsS_SQL
{
    class Program
    {
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
            Console.WriteLine("{0, 20}|{1, 25}|{2,10}|{3,5}||{4,25}|{5,10}|{6,5}\n" +
                "----------------------------------------------------------------------------------------------------------------", "Table", "Column", "Type", "Lenth", "Column", "Type", "Length");
            foreach (var item in diffsInStructure)
            {
                var objSQL = listOfTableAndColsNamesSQL.Find(x => x.Table == item.Key);
                var objORA = listOfTableAndColsNamesORA.Find(x => x.Table == item.Key);
                var c = 0;
                if (objORA != null && objSQL != null)
                {
                    var h = 0;
                    foreach (var i in item.Value)
                    {
                        var list = item.Value.FindAll(x => x.ColumnName == i.ColumnName);
                        if (list.Count > 1)
                        {
                            if (list.First().ColumnType != list.Last().ColumnType)
                            {
                                if (h < item.Value.Count / 2)
                                {

                                    if (c > 0)
                                    {
                                        Console.WriteLine("{0, 20}|{1, 25}|{2,10}|{3,5}||{4,25}|{5,10}|{6,5}|", "", list.First().ColumnName, list.First().ColumnType, list.First().DataLength, list.Last().ColumnName, list.Last().ColumnType, list.Last().DataLength);
                                    }
                                    else
                                    {
                                        Console.WriteLine("{0, 20}|{1, 25}|{2,10}|{3,5}||{4,25}|{5,10}|{6,5}|", item.Key, list.First().ColumnName, list.First().ColumnType, list.First().DataLength, list.Last().ColumnName, list.Last().ColumnType, list.Last().DataLength);
                                    }
                                    c++;
                                    h++;

                                }
                            }
                        }
                        else
                        {
                            if (objSQL.Columns.Contains(list.LastOrDefault()))
                            {
                                Console.WriteLine("{0, 20}|{1, 25}|{2,10}|{3,5}||{4,25}|{5,10}|{6,5}|", item.Key, list.First().ColumnName, list.First().ColumnType, list.First().DataLength, "-------", "--------", "---");
                            }
                            if (objORA.Columns.Contains(list.LastOrDefault()))
                            {
                                Console.WriteLine("{0, 20}|{1, 25}|{2,10}|{3,5}||{4,25}|{5,10}|{6,5}|", item.Key, "-------", "--------", "---", list.First().ColumnName, list.First().ColumnType, list.First().DataLength);
                            }
                        }
                    }
                }
                Console.WriteLine("-------------------------------------------------------------------------------------------------------------");
            }
            Console.ReadKey();
        }
    }
}
