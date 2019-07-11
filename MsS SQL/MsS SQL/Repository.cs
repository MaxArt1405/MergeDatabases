using System;
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
            var from = string.Empty;
            var items = string.Empty;
            if (_type == DbType.MsSQL)
            {
                from = "INFORMATION_SCHEMA.COLUMNS";
                items = "UPPER(TABLE_NAME), UPPER(COLUMN_NAME), UPPER(DATA_TYPE), CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION";
            }
            if (_type == DbType.Oracle)
            {
                from = $"SYS.ALL_TAB_COLUMNS WHERE OWNER = '{_owner}'";
                items = "UPPER(TABLE_NAME), UPPER(COLUMN_NAME), UPPER(DATA_TYPE), DATA_LENGTH";
            }
            var select = $"SELECT {items} FROM {from} ORDER BY TABLE_NAME";

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

                            var length = (reader[3] == DBNull.Value) ? ((reader[4] == DBNull.Value) ? Convert.ToInt32(reader[6]) : 0) : (_type == DbType.Oracle && (string)reader[2] == "NUMBER") ? 0 : Convert.ToInt32(reader[3]);

                            var last = tempData.LastOrDefault();
                            if (last != null && last.Table == reader.GetString(0))
                            {
                                last.Columns.Add(new Column()
                                {
                                    ColumnName = reader.GetString(1),
                                    ColumnType = GetType(reader, _type),
                                    DataLength = length,
                                    DbType = _type
                                });
                            }
                            else
                            {
                                var obj = new TableObject()
                                {
                                    Table = reader.GetString(0),
                                    Columns = new List<Column>()
                                    {
                                        new Column()
                                            {
                                                ColumnName = reader.GetString(1),
                                                ColumnType = GetType(reader,_type),
                                                DataLength = length,
                                                DbType = _type
                                            }
                                        }
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
            foreach (var firstColumn in firstObj.Columns)
            {
                var secondColumn = secondObj.Columns.Find(x => x.ColumnName == firstColumn.ColumnName);
                if (secondColumn != null)
                {
                    if (firstColumn.ColumnType == secondColumn.ColumnType)
                    {
                        if (firstColumn.DataLength != secondColumn.DataLength)
                        {
                            answer.Add(firstColumn);
                        }
                    }
                    else
                    {
                        answer.Add(firstColumn);
                    }
                }
                else
                {
                    answer.Add(firstColumn);
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
        private static Type GetType (IDataReader reader, DbType type)
        {
            if(type == DbType.MsSQL)
            {
                if((string)reader[2] == "NUMERIC")
                {
                    return typeof(int);
                }
                if((string)reader[2] == "INT")
                {
                    return typeof(int);
                }
                if((string)reader[2] == "FLOAT")
                {
                    return typeof(float);
                }
                if((string)reader[2] == "VARACHAR")
                {
                    return typeof(string);
                }
                if((string)reader[2] == "VARBINARY")
                {
                    return typeof(byte[]);
                }
                if((string)reader[2] == "DATETIME2")
                {
                    return typeof(DateTime);
                }
            }
            if(type == DbType.Oracle)
            {
                if ((string)reader[2] == "NUMBER")
                {
                    if(reader[5] != DBNull.Value)
                    {
                        return typeof(int);
                    }
                    else
                    {
                        return typeof(float);
                    }
                }
                if ((string)reader[2] == "CLOB")
                {
                    return typeof(string);
                }
                if ((string)reader[2] == "LONG RAW")
                {
                    return typeof(byte[]);
                }
                if ((string)reader[2] == "FLOAT")
                {
                    return typeof(float);
                }
                if ((string)reader[2] == "VARACHAR2")
                {
                    return typeof(string);
                }
                if ((string)reader[2] == "BLOB")
                {
                    return typeof(byte[]);
                }
                if ((string)reader[2] == "DATE")
                {
                    return typeof(DateTime);
                }
            }
            return typeof(string);
        }
    }
}