using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.OracleClient;
using System.Data.SqlClient;
using System.Linq;

namespace MsS_SQL
{
    public class Repository
    {
        private readonly string _connection;
        private readonly DbType _type;
        private readonly string _owner;

        public Repository(string connectionString, DbType currentType, string owner)
        {
            _owner = owner;
            _connection = connectionString;
            _type = currentType;
        }

        public List<string> GetTableNames()
        {
            var from = "";
            if (_type == DbType.MsSQL)
            {
                from = "INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE ='BASE TABLE'";
            }
            if (_type == DbType.Oracle)
            {
                from = $"ALL_TABLES WHERE OWNER = '{_owner}'";
            }
            var select = $"SELECT UPPER(TABLE_NAME) FROM {from} ORDER BY TABLE_NAME";

            var tempData = new List<string>();
            using (var connection = GetConnection(_connection, _type))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = select;
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
        public List<TableObject> GetListOfColsAndTables()
        {
            var from = "";
            if (_type == DbType.MsSQL)
            {
                from = "INFORMATION_SCHEMA.COLUMNS";
            }
            if (_type == DbType.Oracle)
            {
                from = $"SYS.ALL_TAB_COLUMNS WHERE OWNER = '{_owner}'";
            }
            var select = $"SELECT UPPER(TABLE_NAME), UPPER(COLUMN_NAME), UPPER(DATA_TYPE) FROM {from} ORDER BY TABLE_NAME";

            var tempData = new List<TableObject>();
            using (var connection = GetConnection(_connection, _type))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = select;
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var last = tempData.LastOrDefault();
                            if (last != null && last.Table == reader.GetString(0))
                            {
                                last.Columns.Add(new Column()
                                {
                                    ColumnName = reader.GetString(1),
                                    ColumnType = reader.GetString(2)
                                });
                            }
                            else
                            {
                                var obj = new TableObject
                                {
                                    Table = reader.GetString(0),
                                    Columns = new List<Column>() { new Column() { ColumnName = reader.GetString(1), ColumnType = reader.GetString(2) } }
                                };
                                tempData.Add(obj);
                            }
                        }
                    }
                    return tempData;
                }
            }
        }

        public Dictionary<string, List<string>> FindDifferenceInTablesDict(List<string> FirstList, List<string> SecondList)
        {
            var commonTables = FirstList.Intersect(SecondList).ToList();

            var answer = new Dictionary<string, List<string>>
            {
                { "Has SQL not Oracle", FirstList.Except(commonTables).ToList() },
                { "Has Oracle not SQL", SecondList.Except(commonTables).ToList() }
            };
            return answer;
        }
        public List<string> FindDifferenceInTablesList(List<string> FirstList, List<string> SecondList)
        {
            var commonTables = FirstList.Intersect(SecondList).ToList();

            var HasSQl = FirstList.Except(commonTables).ToList();
            var HasOra = SecondList.Except(commonTables).ToList();
            var answer = HasSQl.Union(HasOra).ToList();
            answer.Sort();
            return answer;
        }

        public Dictionary<string, List<Column>> FindDifferenceInCols(List<TableObject> FirstTable, List<TableObject> SecondTable)
        {
            if (FirstTable != null && SecondTable != null)
            {
                if (FirstTable.Any() && SecondTable.Any())
                {
                    return SearchDifference(FirstTable, SecondTable);
                }
            }
            return new Dictionary<string, List<Column>>();
        }       

        private Dictionary<string, List<Column>> SearchDifference(List<TableObject> FirstTable, List<TableObject> SecondTable)
        {
            var diffs = new Dictionary<string, List<Column>>();
            foreach (var item in FirstTable)
            {
                if (SecondTable.Exists(x => x.Table == item.Table))
                {
                    var tmpORA = SecondTable.Find(x => x.Table == item.Table);

                    var SQLORA = CompareColumns(item, tmpORA);
                    var ORASQL = CompareColumns(tmpORA, item);

                    var difference = SQLORA.Union(ORASQL).ToList();

                    if (difference != null && difference.Any())
                    {
                        diffs.Add(item.Table, difference);
                    }
                }
            }
            return diffs;
        }
        private List<Column> CompareColumns(TableObject firstObj, TableObject secondObj)
        {
            var answer = new List<Column>();
            foreach (var column in firstObj.Columns)
            {
                var columnORA = secondObj.Columns.Find(x => x.ColumnName == column.ColumnName);
                if (columnORA != null)
                {
                    if (column.ColumnName == columnORA.ColumnName)
                    {
                        if (accordance.Keys.Contains(columnORA.ColumnType))
                        {
                            accordance.TryGetValue(columnORA.ColumnType, out List<string> value);
                            if (value != null && value.Any())
                            {
                                if (!value.Contains(column.ColumnType))
                                {
                                    answer.Add(column);
                                }
                            }
                        }                       
                    }
                    else
                    {
                        answer.Add(column);
                    }
                }
                else
                {
                    answer.Add(column);
                }
            }
            return answer;
        }

        private static IDbConnection GetConnection(string connectionString, DbType currentDbType)
        {
            if (currentDbType == DbType.Oracle)
                return new OracleConnection(connectionString);
            if (currentDbType == DbType.MsSQL)
                return new SqlConnection(connectionString);
            throw new ConfigurationException();
        }
        private readonly Dictionary<string, List<string>> accordance = new Dictionary<string, List<string>>
        {
            {"BLOB", new List<string>(){ "VARBINARY" }},
            {"CLOB", new List<string>(){ "VARCHAR" }},
            {"DATE", new List<string>(){ "DATETIME2" }},
            {"NUMBER", new List<string>(){ "INT", "NUMERIC", "DECIMAL" } },
            {"FLOAT", new List<string>(){ "FLOAT" } },
            {"LONGRAW", new List<string>(){ "VARBINARY" } },
            {"VARCHAR2", new List<string>(){ "VARCHAR" } },
            {"VARBINARY", new List<string>(){ "BLOB", "LONGRAW" }},
            {"VARCHAR", new List<string>(){ "CLOB","VARCHAR2" }},
            {"DATETIME2", new List<string>(){ "DATE" }},
            {"INT", new List<string>(){ "NUMBER" }},
            {"NUMERIC", new List<string>(){ "NUMBER" }},
            {"DECIMAL", new List<string>(){ "NUMBER" }},
        };
    }
}
