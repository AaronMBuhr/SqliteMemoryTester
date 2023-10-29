using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static SqliteMemoryTester.Debug;

namespace SqliteMemoryTester
{
    public class Tester
    {
        SqliteConnection db_connection_;

        public void ExecuteSql(string sql, IDictionary<string, object> parms)
        {
            using (var cmd = db_connection_.CreateCommand())
            {
                cmd.CommandText = sql;
                if (parms != null)
                {
                    foreach (var pair in parms)
                    {
                        cmd.Parameters.AddWithValue("$" + pair.Key, pair.Value);
                    }
                }

                db_connection_.Open();
                cmd.ExecuteNonQuery();
                //db_connection_.Close();
            }
        }

        public object QuerySqlValue(string sql, IDictionary<string, object> parms)
        {
            object result;
            using (var cmd = db_connection_.CreateCommand())
            {
                cmd.CommandText = sql;
                if (parms != null)
                {
                    foreach (var pair in parms)
                    {
                        cmd.Parameters.AddWithValue("$" + pair.Key, pair.Value);
                    }
                }

                db_connection_.Open();
                result = cmd.ExecuteScalar();
                //db_connection_.Close();
            }
            return result;
        }

        public IEnumerable<object[]> QuerySqlValues(string sql, IDictionary<string,object> parms)
        {
            using (var cmd = db_connection_.CreateCommand())
            {
                cmd.CommandText = sql;
                if (parms != null)
                {
                    foreach (var pair in parms)
                    {
                        cmd.Parameters.AddWithValue("$" + pair.Key, pair.Value);
                    }
                }

                object[] values = null;
                int fieldCount = -1;
                db_connection_.Open();
                using (var rdr = cmd.ExecuteReader())
                {
                    if (values == null)
                    {
                        fieldCount = rdr.FieldCount;
                        values = new object[rdr.FieldCount];
                    }
                    if (rdr.GetValues(values) != fieldCount)
                    {
                        throw new Exception("QuerySqlValues error");
                    }
                    yield return values;
                }
                //db_connection_.Close();
            }
        }

        public void OpenDatabase()
        {
            SQLitePCL.Batteries.Init();
            var connectionStringBuilder = new SqliteConnectionStringBuilder { DataSource = @":memory:" };
            db_connection_ = new SqliteConnection(connectionStringBuilder.ToString());

            //Debugger.Break();
        }

        public void CreateTables()
        {
            var sqlstr = @"
create table Host (
    host_id integer NOT NULL CONSTRAINT ""PK_Host"" PRIMARY KEY AUTOINCREMENT,
    name varchar(100) not null
);
create table Program (
    program_id integer NOT NULL CONSTRAINT ""PK_Program"" PRIMARY KEY AUTOINCREMENT,
    name varchar(100) not null
);
create table Log_Event(
    log_event_id integer NOT NULL CONSTRAINT ""PK_Program"" PRIMARY KEY AUTOINCREMENT,
    timestamp integer not null, 
    host_id integer, 
    program_id integer, 
    message_id integer not null
);
create index log_event_idx_timestamp on Log_Event(timestamp);
create index log_event_idx_host_id on Log_Event(host_id);
create index log_event_idx_program_id on Log_Event(program_id);
create index log_event_idx_message_id on Log_Event(message_id);
";
            ExecuteSql(sqlstr, null);

            //ExecuteSql("create table Host(host_id integer, name varchar(200));", null);

            //Debugger.Break();
        }

        public void GetTables()
        {
            db_connection_.Open();
            Debugger.Break();
            System.Data.DataTable tables = db_connection_.GetSchema("Tables");
            System.Data.DataTable columns = db_connection_.GetSchema("Columns");
            //db_connection_.Close();
        }

        public int AddHost(string host_name)
        {
            var result = QuerySqlValue(
                "insert into Host (name) values ($host_name); select last_insert_rowid();",
                new Dictionary<string, object> { { "host_name", host_name } });
            return Convert.ToInt32(result);
        }

        public int AddProgram(string program_name)
        {
            var result = QuerySqlValue(
                "insert into Program (name) values ($program_name); select last_insert_rowid();",
                new Dictionary<string, object> { { "program_name", program_name } });
            return Convert.ToInt32(result);
        }

        public int AddLogEvent(int host_id, int program_id, int message_id)
        {
            string sql = @"
insert into Log_Event
(
    timestamp,
    host_id,
    program_id,
    message_id
) values (
    $timestamp,
    $host_id,
    $program_id,
    $message_id
);
select last_insert_rowid();
";
            int timestamp = (int)DateTimeOffset.Now.ToUnixTimeSeconds() % int.MaxValue;
            var parms = new Dictionary<string, object>
            {
                { "timestamp", timestamp },
                { "host_id", host_id },
                { "program_id", program_id },
                { "message_id", message_id }
            };

            return Convert.ToInt32(QuerySqlValue(sql, parms));
        }
    }
}
