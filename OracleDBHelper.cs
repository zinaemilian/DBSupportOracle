using Oracle.ManagedDataAccess.Client;
using OracleHelpers.Utils;
using System;
using System.Collections.Generic;

namespace OracleHelpers.Helpers
{
    public class OracleDBHelper : Disposer
    {
        private OracleQueryHelper _queryhelper;

        public OracleDBHelper()
        {
            _queryhelper = new OracleQueryHelper();
        }

        public Dictionary<string, string> SelectDBRecord(string connectionString, string query)
        {
            Dictionary<string, string> valuesFromDB = new Dictionary<string, string>();
            using (OracleConnection conneection = new OracleConnection(connectionString))
            {
                OracleHelper.Connect(conneection);
                using (OracleCommand command = new OracleCommand(query, conneection))
                using (OracleDataReader readRecord = command.ExecuteReader())
                    if (readRecord.Read())
                        for (int i = 0; i <= readRecord.FieldCount - 1; i++)
                        {
                            var listItem = readRecord.GetValue(i);
                            if (string.IsNullOrEmpty(listItem.ToString()))
                                listItem = "EMPTY";
                            valuesFromDB.Add(readRecord.GetName(i), listItem.ToString());
                        }
            }
            return valuesFromDB;
        }

        public List<Dictionary<string, string>> SelectMultipleDbRecord(string connectionString, string query)
        {
            List<Dictionary<string, string>> recordHistory = new List<Dictionary<string, string>>();
            using OracleConnection conneection = new OracleConnection(connectionString);
            OracleHelper.Connect(conneection);
            using OracleCommand command = new OracleCommand(query, conneection);
            using OracleDataReader readRecord = command.ExecuteReader();
            while (readRecord.Read())
            {
                var valuesFromDb = new Dictionary<string, string>();
                for (var i = 0; i <= readRecord.FieldCount - 1; i++)
                {
                    var listItem = readRecord.GetValue(i);

                    try
                    {
                        valuesFromDb.Add(readRecord.GetName(i), listItem.ToString());
                    }
                    catch (Exception ex)
                    {
                        if ((ex.Message.Contains(
                            "An item with the same key has already been added. Key: SKYGENRE")) || (ex.Message.Contains(
                            "An item with the same key has already been added. Key: YVGENRE")))
                        {
                            Console.WriteLine($"Accepted Exception {ex.Message}");
                        }
                        else
                        {
                            throw;
                        }

                    }
                }
                recordHistory.Add(valuesFromDb);
            }

            return recordHistory;
        }

        public void InsertDBRecord(string connectionString, string query)
        {
            using (OracleConnection conneection = new OracleConnection(connectionString))
            {
                OracleHelper.Connect(conneection);
                conneection.BeginTransaction();
                using (OracleCommand command = new OracleCommand(query, conneection))
                {
                    command.ExecuteNonQuery();
                    command.Transaction.Commit();
                }
            }
        }

        public void UpdateDBRecord(string connectionString, string query)
        {
            using (OracleConnection _conn = new OracleConnection(connectionString))
            {
                OracleHelper.Connect(_conn);
                _conn.BeginTransaction();
                using (OracleCommand _cmd = new OracleCommand(query, _conn))
                {
                    _cmd.ExecuteNonQuery();
                    _cmd.Transaction.Commit();
                }
            }
        }

        public void DeleteDBRecord(string connectionString, string query)
        {
            using (OracleConnection conneection = new OracleConnection(connectionString))
            {
                OracleHelper.Connect(conneection);
                conneection.BeginTransaction();
                using (OracleCommand command = new OracleCommand(query, conneection))
                {
                    command.ExecuteNonQuery();
                    command.Transaction.Commit();
                }
            }
        }
    }
}
