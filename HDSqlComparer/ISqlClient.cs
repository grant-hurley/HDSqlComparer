using System.Data;

namespace HDSqlComparer
{
    public interface ISqlClient
    {
        DataTable GetDataTable(string sql, object parameters = null, int retryCount = 5);
    }
}
