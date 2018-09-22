using System;
using System.Data;
using Dapper;
using MySql.Data.MySqlClient;

namespace HDSqlComparer
{
    public class MySqlClient : ISqlClient
    {
        private string _connectionString;
        public MySqlClient(string connectionString)
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
                    using (var connection = new MySqlConnection(_connectionString))
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
