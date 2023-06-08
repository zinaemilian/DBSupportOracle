namespace DefaultNamespace;

public class DatabaseFactory
{
    public static IDatabase CreateDatabase(string type)
    {
        switch (type.ToLower())
        {
            case "sqlite":
                return new SqliteDatabase();
            case "sqlserver":
                return new SqlServerDatabase();
            default:
                throw new Exception("Invalid database type");
        }
    }
}
/*IDatabase db = DatabaseFactory.CreateDatabase("sqlite");
db.Connect();

// perform database operations

db.Disconnect();
*/