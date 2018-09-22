using System;
using System.Collections.Generic;
using System.Data;

namespace HDSqlComparer
{
    public class SqlComparison
    {
        public object Parameters { get; set; }
        public DataTable MasterSqlResult { get; set; }
        public DataTable BranchSqlResult { get; set; }
        public TimeSpan MasterSqlExecutionTime { get; set; }
        public TimeSpan BranchSqlExecutionTime { get; set; }
        public bool ExactMatch { get; set; } = true;
        public List<string> SummaryFindings { get; set; } = new List<string>();
        public List<string> DetailedFindings { get; set; } = new List<string>();

        public Guid Id { get; set; } = Guid.NewGuid();

        public void AnalyseSql()
        {
            ExactMatch = true;
            SetupHeader();
            CompareExecutionTime();
            CompareRowCounts();
            CompareIndividualRows();
            AddMatchResult();
        }

        private void AddMatchResult()
        {
            SummaryFindings.Add($"Results match? {(ExactMatch ? "YES" : "NO")}");
        }

        private void SetupHeader()
        {
            SummaryFindings.Add($"Detailed result id: '{Id}'");
            SummaryFindings.Add($"Parameters: {Newtonsoft.Json.JsonConvert.SerializeObject(Parameters)}");
        }

        private void CompareIndividualRows()
        {
            DetailedFindings.Add($"Detailed result id: '{Id}'");

            bool branchRunOutLogged = false;
            for (int i = 0; i < MasterSqlResult.Rows.Count; i++)
            {
                if (BranchSqlResult.Rows.Count >= i + 1)
                {
                    CompareRow(MasterSqlResult.Rows[i], BranchSqlResult.Rows[i]);
                }
                else
                {
                    if (!branchRunOutLogged)
                    {
                        DetailedFindings.Add("Branch sql does not contain any more rows");
                        branchRunOutLogged = true;
                        ExactMatch = false;
                    }
                }
            }

            if (BranchSqlResult.Rows.Count > MasterSqlResult.Rows.Count)
            {
                DetailedFindings.Add("Branch sql still has additional rows");
                ExactMatch = false;
            }

        }

        private void CompareRow(DataRow masterRow, DataRow branchRow)
        {
            string rowDifs = "";
            var dataColumns = masterRow.Table.Columns;
            foreach (DataColumn dc in dataColumns)
            {
                object branchValue = null;
                if (TryFindValue(branchRow, dc.ColumnName, out branchValue))
                {
                    if (branchValue.ToString() != masterRow[dc.ColumnName].ToString())
                    {
                        rowDifs += $"{dc.ColumnName} differs: {masterRow[dc.ColumnName]} (M) - {branchValue} (B), ";
                        ExactMatch = false;
                    }
                }
                else
                {
                    rowDifs += $"{dc.ColumnName} missing from branch, ";
                    ExactMatch = false;
                }
            }

            foreach (DataColumn dc in branchRow.Table.Columns)
            {
                object masterValue = null;
                if (!TryFindValue(masterRow, dc.ColumnName, out masterValue))
                {
                    rowDifs += $"{dc.ColumnName} missing from master, ";
                    ExactMatch = false;
                }
            }

            if (!string.IsNullOrEmpty(rowDifs))
            {
                DetailedFindings.Add(rowDifs);
            }
        }

        private bool TryFindValue(DataRow row, string columnName, out object value)
        {
            try
            {
                value = row[columnName];
                return true;
            }
            catch
            {
                value = null;
                return false;
            }
        }

        private void CompareExecutionTime()
        {
            SummaryFindings.Add($"Execution times: {MasterSqlExecutionTime.TotalSeconds} (M), {BranchSqlExecutionTime.TotalSeconds} (B)");
        }

        private void CompareRowCounts()
        {
            var masterRowCount = MasterSqlResult?.Rows.Count;
            var branchRowCount = BranchSqlResult?.Rows.Count;

            if (masterRowCount != branchRowCount)
            {
                SummaryFindings.Add($"Row counts differ: {masterRowCount} (M), {branchRowCount} (B)");
                ExactMatch = false;
            }
            else
            {
                SummaryFindings.Add($"Row counts identical: {masterRowCount}");
            }
        }
    }
}
