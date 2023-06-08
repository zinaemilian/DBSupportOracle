namespace DefaultNamespace;

public class DBHelper
{
    public List<Dictionary<string, string>> SelectMultipleRecords(string connectionString, string query)
    {
        var recordHistory = new List<Dictionary<string, string>>();

        using (var conneection = new OracleConnection(connectionString))
        {
            OracleHelper.Connect(conneection);

            using (var command = new OracleCommand(query, conneection))
            using (var readRecord = command.ExecuteReader())
            {
                while (readRecord.Read())
                {
                    var valuesFromDb = new Dictionary<string, string>();

                    for (var i = 0; i <= readRecord.FieldCount - 1; i++)
                    {
                        var listItem = readRecord.GetValue(i);
                        var key = readRecord.GetName(i);
                        var value = listItem?.ToString();

                        if (valuesFromDb.ContainsKey(key))
                        {
                            valuesFromDb[key] = valuesFromDb[key] + "*" + value;
                        }
                        else
                        {
                            valuesFromDb.Add(key, value);
                        }
                    }

                    recordHistory.Add(valuesFromDb);
                }
            }
        }
        return recordHistory;
    }

}
/*
 * This SelectMultipleRecords method is used to retrieve multiple records from an Oracle database and store them in a List of Dictionary objects. The method takes two parameters:

connectionString: a string containing the connection details to the Oracle database.
query: a string containing the SQL query to execute on the database.
The method starts by initializing an empty list recordHistory of Dictionary objects, which will store the records retrieved from the database.

Next, the method opens a connection to the database using the connectionString and OracleConnection class. The method then creates an OracleCommand object using the query and the open connection.

The method uses the ExecuteReader method of the OracleCommand object to execute the query and retrieve the results. The ExecuteReader method returns a OracleDataReader object that is used to iterate through the records retrieved from the database.

In the while loop, for each record, the method creates a new Dictionary object valuesFromDb to store the values of the columns for that record.

For each column in the record, the method retrieves the column name and its value using the GetName and GetValue methods of the OracleDataReader object. The value is then converted to a string and added to the valuesFromDb dictionary using the Add method.

If the same column name already exists in the valuesFromDb dictionary, the value of the existing key is concatenated with the new value, separated by a comma.

Finally, the valuesFromDb dictionary is added to the recordHistory list and the while loop continues until all records have been processed.

Once the loop has finished, the method returns the recordHistory list, which contains all the records retrieved from the database, stored as Dictionary objects.
 * 
 */