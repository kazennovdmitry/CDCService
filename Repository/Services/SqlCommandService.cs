using System.Data;
using Microsoft.Data.SqlClient;

namespace CDC.Repository.Services;

public static class SqlCommandService
{
    public static SqlCommand GetSqlCommand(SqlConnection connection, string sqlQuery)
    {
        var command = new SqlCommand();
        command.Connection = connection;
        command.CommandType = CommandType.Text;
        command.CommandText = sqlQuery;
        return command;
    }
}