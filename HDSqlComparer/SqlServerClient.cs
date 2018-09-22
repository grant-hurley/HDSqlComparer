using System;
using System.Data;
using System.Data.SqlClient;
using Dapper;

namespace HDSqlComparer
{
    public class SqlServerClient : ISqlClient
    {
        private string _connectionString;
        public SqlServerClient(string connectionString)
        {
            _connectionString = connectionString;
        }

        public DataTable GetDataTable(string sql, object parameters = null, int retryCount = 5)
        {
            var retries = 0;
            while (retries < retryCount)
            {
                retries++;
                try
                {
                    using (var connection = new SqlConnection(_connectionString))
                    {
                        var reader = connection.ExecuteReader(sql, parameters);
                        var dataTable = new DataTable();
                        dataTable.Load(reader);

                        return dataTable;
                    }
                }
                catch (Exception)
                {
                    if(retries >= retryCount)
                    {
                        throw;
                    }
                }
            }

            return new DataTable();
        }
    }
}
