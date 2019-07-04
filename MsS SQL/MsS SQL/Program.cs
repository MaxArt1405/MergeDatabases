using System;

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
            
            var SQLrepository = new Repository(connectionSQL, DbType.MsSQL);
            var ORArepository = new Repository(connectionOracle, DbType.Oracle);

            var listOfTableNamesSQL = SQLrepository.GetTableNames(msOwner);
            var listOfTableNamesOracle = ORArepository.GetTableNames(oraOwner);

            var Diff = SQLrepository.FindDifferenceInTables(listOfTableNamesSQL, listOfTableNamesOracle);

            var listOfTableAndColsNamesSQL = SQLrepository.GetListOfColsAndTables(msOwner);
            var listOfTableAndColsNamesORA = ORArepository.GetListOfColsAndTables(oraOwner);

            var diffsInStructure = SQLrepository.FindDifferenceInCols(listOfTableAndColsNamesSQL, listOfTableAndColsNamesORA);

            foreach(var item in Diff)
            {
                Console.WriteLine("{0, 30}     |{1, 30}", item.Key, "Tables");
                foreach(var i in item.Value)
                {
                    Console.WriteLine("{0, 30}     |{1, 30}", "", i);
                }
                Console.WriteLine("-------------------------------------------------------------------");
            }

            foreach(var sql in listOfTableNamesSQL)
            {
                foreach(var oracle in listOfTableNamesOracle)
                {
                    if (!listOfTableNamesSQL.Contains(oracle))
                    {
                        Console.WriteLine("{0,30}       |{1,30}", "-----------------", oracle);
                    }
                    if (!listOfTableNamesOracle.Contains(sql))
                    {
                        Console.WriteLine("{0,30}       |{1,30}", sql, "-----------------");
                    }                 
                }
            }
            Console.ReadKey();
        }
    }
}
