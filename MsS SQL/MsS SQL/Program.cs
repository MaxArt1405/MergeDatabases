using System.Collections.Generic;
using System.Data.OracleClient;
using System.Data.SqlClient;
using System.Linq;

namespace MsS_SQL
{
    class Program
    {
        public static void Main(string[] args)
        {
            var msOwner = "ADSPACE_MPHH";
            var oraOwner = "ADSPACE_MC";
            var connectionSTR = $"Data Source=ADV09;Initial Catalog={msOwner};User ID={msOwner};Password={msOwner}";
            var connectionOracle = $"Data Source=ADV02;Password={oraOwner};Persist Security Info=True;User ID=ADSPACE_MC;";

            var queryForORA = "SELECT UPPER(table_name) FROM all_tables where owner = 'ADSPACE_MC' order by table_name";
            var queryForSQL = "SELECT UPPER(table_name) FROM ADSPACE_MPHH.INFORMATION_SCHEMA.TABLES order by table_name";

            var listOfTablesSQL = GetResultsSQL(connectionSTR, queryForSQL);
            var listOfTablesOracle = GetResultsORACLE(connectionOracle, queryForORA);
    
            var commonTables = listOfTablesSQL.Intersect(listOfTablesOracle).ToList();
            var HasOracleNotSQL = listOfTablesSQL.Except(commonTables).ToList();
            var HasSQLNotOracle = listOfTablesOracle.Except(commonTables).ToList();

            var diffsInStructure = AreEqual(connectionSTR, connectionOracle, commonTables);
            var tables = HasOracleNotSQL.Union(HasSQLNotOracle).ToList();
        }
        public static List<string> GetResultsSQL(string connectionSTR, string query)
        {
            var tempData = new List<string>();
            using (var connection = new SqlConnection(connectionSTR))
            {
                connection.Open();
                using (var command = new SqlCommand(query, connection))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            tempData.Add(reader.GetString(0));
                        }
                    }
                    return tempData;
                }
            }
        }
        public static List<string> GetResultsORACLE(string connectionSTR, string query)
        {
            var tempData = new List<string>();
            using (var connection = new OracleConnection(connectionSTR))
            {
                connection.Open();
                using (var command = new OracleCommand(query, connection))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            tempData.Add(reader.GetString(0));
                        }
                    }
                    return tempData;
                }
            }
        }
        public static List<TableObject> GetListOfColsAndTablesORA(string connectionSTR, string query)
        {
            var tempData = new List<TableObject>();
            using (var connection = new OracleConnection(connectionSTR))
            {
                connection.Open();
                using (var command = new OracleCommand(query, connection))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var last = tempData.LastOrDefault();
                            if (last != null && last.Table == reader.GetString(0))
                            {
                                last.Columns.Add(reader.GetString(1));
                            }
                            else
                            {
                                var list = new List<string>();
                                var obj = new TableObject
                                {
                                    Table = reader.GetString(0)
                                };
                                obj.Columns = list;
                                tempData.Add(obj);
                            }
                        }
                    }
                     return tempData;
                }
            }
        }
        public static List<TableObject> GetListOfColsAndTablesSQL(string connectionSTR, string query)
        {
            var tempData = new List<TableObject>();
            using (var connection = new SqlConnection(connectionSTR))
            {
                connection.Open();
                using (var command = new SqlCommand(query, connection))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var last = tempData.LastOrDefault();
                            if (last != null && last.Table == reader.GetString(0))
                            {
                                last.Columns.Add(reader.GetString(1));
                            }
                            else
                            {
                                var list = new List<string>();
                                var obj = new TableObject
                                {
                                    Table = reader.GetString(0)
                                };
                                obj.Columns = list;
                                tempData.Add(obj);
                            }
                        }
                    }
                    return tempData;
                }
            }
        }
        public static Dictionary<string,List<string>> AreEqual(string connectionToSQL, string connectionToORA, List<string> commonTables)
        {
            var diffs = new Dictionary<string, List<string>>();
            var listOFTablesORA = GetListOfColsAndTablesORA(connectionToORA, $"SELECT UPPER(table_name), UPPER(column_name) FROM SYS.ALL_TAB_COLUMNS t where owner = 'ADSPACE_MC' order by t.table_name");
            var listOFTablesSQL = GetListOfColsAndTablesSQL(connectionToSQL, $"SELECT UPPER(t.table_name) , UPPER(c.column_name) FROM ADSPACE_MPHH.INFORMATION_SCHEMA.COLUMNS c JOIN ADSPACE_MPHH.INFORMATION_SCHEMA.TABLES t on t.table_name = c.table_name ");
            foreach(var item in commonTables)
            {
                if(listOFTablesSQL.Exists(x => x.Table == item) && listOFTablesORA.Exists(x => x.Table == item))
                {
                    var tmpSQL = listOFTablesSQL.Find(x => x.Table == item);
                    var tmpORA = listOFTablesORA.Find(x => x.Table == item);

                    var intersect = tmpSQL.Columns.Intersect(tmpORA.Columns).ToList();

                    var elementsInSQL = tmpSQL.Columns.Except(intersect).ToList();
                    var elementsInORA = tmpORA.Columns.Except(intersect).ToList();

                    var difference = elementsInORA.Union(elementsInSQL).ToList();
                    if (difference != null && difference.Any())
                    {
                        diffs.Add(item, difference);
                    }
                }
            }
            return diffs;
        }
    }
    public class TableObject
    {
        public string Table { get; set; }
        public List<string> Columns { get; set; }
    }
}
