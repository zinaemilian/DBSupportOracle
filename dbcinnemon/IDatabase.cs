namespace DefaultNamespace;

public interface IDatabase
{
    void Connect();
    void Disconnect();
    void Insert(string tableName, Dictionary<string, string> data);
    DataTable Select(string tableName);
    void Update(string tableName, Dictionary<string, string> data, string condition);
    void Delete(string tableName, string condition);
}