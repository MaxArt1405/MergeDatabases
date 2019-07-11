using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MsS_SQL
{
    public class Column
    {
        public string ColumnName { get; set; }
        public Type ColumnType { get; set; }        
        public DbType DbType { get; set; }
        public int DataLength { get; set; }
    }
}
