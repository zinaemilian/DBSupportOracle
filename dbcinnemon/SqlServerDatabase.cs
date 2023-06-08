namespace DefaultNamespace;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Generic;

public class SqlServerDatabase : IDatabase
{
    private SqlConnection connection;

    public void Connect()
    {
        connection = new SqlConnection("Data Source=(local);Initial Catalog=database;Integrated Security=True");
        connection.Open();
    }

    public void Disconnect()
    {
        connection.Close();
    }

    public void Insert(string tableName, Dictionary<string, string> data)
    {
        string columns = string.Join(",", data.Keys);
        string values = string.Join(",", data.Values.Select(x => $"'{x}'"));

        string query = $"INSERT INTO {tableName} ({columns}) VALUES ({values})";

        using (SqlCommand command = new SqlCommand(query, connection))
        {
            command.ExecuteNonQuery();
        }
    }

    public DataTable Select(string tableName)
    {
        DataTable result = new DataTable();

        string query = $"SELECT * FROM {tableName}";

        using (SqlCommand command = new SqlCommand(query, connection))
        {
            using (SqlDataAdapter adapter = new SqlDataAdapter(command))
            {
                adapter.Fill(result);
            }
        }

        return result;
    }

    public void Update(string tableName, Dictionary<string, string> data, string condition)
    {
        string setValues = string.Join(",", data.Select(x => $"{x.Key}='{x.Value}'"));

        string query = $"UPDATE {tableName} SET {setValues} WHERE {condition}";

        using (SqlCommand command = new SqlCommand(query, connection))
        {
            command.ExecuteNonQuery();
        }
    }

    public void Delete(string tableName, string condition)
    {
        string query = $"DELETE FROM {tableName} WHERE {condition}";

        using (SqlCommand command = new SqlCommand(query, connection))
        {
            command.ExecuteNonQuery();
        }
    }
}
