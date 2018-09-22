using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;

namespace HDSqlComparer
{
    public class SqlResult
    {
        public DataTable Data { get; set; }
        public TimeSpan ExecutionTime { get; set; }
    }

    public interface ISqlComparer
    {
        SqlComparison CompareSqlQueries(
            string masterSql, 
            string branchSql, 
            object parameters = null);

        List<SqlComparison> CompareSqlQueriesUsingParamsFromDataSource(
            string masterSql,
            string branchSql,
            string dataSourceQuery,
            Action<int> progressReporter);
    }

    public class SqlComparer : ISqlComparer
    {
        private ISqlClient _sqlClient { get; set; }

        public SqlComparer(ISqlClient sqlClient)
        {
            _sqlClient = sqlClient;
        }

        /// <summary>
        /// This will execute both sql queries and compare the results of the data
        /// </summary>
        /// <param name="masterSql">Trusted Sql. Enter you production sql here</param>
        /// <param name="branchSql">Sql to compare against the trusted sql</param>
        /// <param name="parameters">Optional. Will be passed into the query</param>
        /// <returns></returns>
        public SqlComparison CompareSqlQueries(
            string masterSql,
            string branchSql,
            object parameters = null)
        {
            if (string.IsNullOrEmpty(masterSql))
            {
                throw new ArgumentException("Please specify master Sql");
            }

            if (string.IsNullOrEmpty(branchSql))
            {
                throw new ArgumentException("Please specify branch Sql");
            }

            var masterResult = ExecuteSql(
                masterSql, 
                parameters);

            var branchResult = ExecuteSql(
                branchSql, 
                parameters);

            var result = new SqlComparison
            {
                MasterSqlResult = masterResult.Data,
                MasterSqlExecutionTime = masterResult.ExecutionTime,
                BranchSqlResult = branchResult.Data,
                BranchSqlExecutionTime = branchResult.ExecutionTime,
                Parameters = parameters
            };

            result.AnalyseSql();

            return result;
        }

        /// <summary>
        /// This will first execute the 'dataSourceQuery' and use the results of that query as parameters to be passed into the 'masterSql' and 'branchSql'. The 'branchSql' will be evaluated against the 'masterSql' once for every row in the 'dataSourceQuery' results
        /// </summary>
        /// <param name="masterSql">Trusted Sql. Enter you production sql here</param>
        /// <param name="branchSql">Sql to compare against the trusted sql</param>
        /// <param name="dataSourceQuery">Sql to use in order to generate the number of runs and the parameters used in each</param>
        /// <param name="progressReporter">This will be called with the percentage completion of the entire operation</param>
        /// <returns></returns>
        public List<SqlComparison> CompareSqlQueriesUsingParamsFromDataSource(
            string masterSql,
            string branchSql,
            string dataSourceQuery,
            Action<int> progressReporter)
        {
            if (string.IsNullOrEmpty(masterSql))
            {
                throw new ArgumentException("Please specify master Sql");
            }

            if (string.IsNullOrEmpty(branchSql))
            {
                throw new ArgumentException("Please specify branch Sql");
            }

            if (string.IsNullOrEmpty(dataSourceQuery))
            {
                throw new ArgumentException("Please specify data source Sql");
            }

            var dataSource = _sqlClient.GetDataTable(dataSourceQuery);

            int done = 0;
            var results = new List<SqlComparison>();
            foreach (DataRow dataRow in dataSource.Rows)
            {
                done++;
                var paramaters = GetParamsFromDataRow(dataRow);

                var result = CompareSqlQueries(masterSql, branchSql, paramaters);
                results.Add(result);
                progressReporter((int)((decimal)done / dataSource.Rows.Count * 100));
            }

            return results;
        }

        private object GetParamsFromDataRow(DataRow row)
        {
            var dictionary = new Dictionary<string, object>();
            foreach (DataColumn col in row.Table.Columns)
            {
                dictionary.Add(col.ColumnName, row[col.ColumnName]);
            }
            return dictionary;
        }
        private SqlResult ExecuteSql(
            string sql, 
            object parameters)
        {
            var timer = new Stopwatch();
            timer.Start();

            var dt = _sqlClient.GetDataTable(sql, parameters);

            timer.Stop();
            return new SqlResult
            {
                Data = dt,
                ExecutionTime = timer.Elapsed
            };
        }
    }
}
