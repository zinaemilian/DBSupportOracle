using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using OracleHelpers.Utils;
using System;
using System.Collections.Generic;
using System.Data;

namespace OracleHelpers.Helpers
{
    public class OracleSPHelper : Disposer
    {
        public bool EnqueueMessage(string connectionString, string packageName, string procedureName, string symbolicQueueName, string payload)
        {
            using (OracleConnection connection = new OracleConnection(connectionString))
            {
                try
                {
                    OracleHelper.Connect(connection);
                    OracleTransaction transaction = connection.BeginTransaction();
                    OracleCommand command = connection.CreateCommand();
                    command.CommandText = $"{packageName}.{procedureName}";
                    command.CommandType = CommandType.StoredProcedure;
                    command.Parameters.Add("p_symbolic_queue_name", OracleDbType.Varchar2, symbolicQueueName, ParameterDirection.Input);
                    command.Parameters.Add("p_payload", OracleDbType.Clob, payload, ParameterDirection.InputOutput);
                    command.Parameters.Add("p_message_handle", OracleDbType.Blob, ParameterDirection.Output);
                    command.ExecuteNonQuery();
                    transaction.Commit();
                    return true;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"EnqueuMessage has failed due to :  {ex.InnerException ?? ex}");
                    return false;
                }
            }
        }

        public string DequeueMessage(string connectionString, string packageName, string procedureName, string symbolicQueueName, int waitSeconds = 50)
        {
            using (OracleConnection connection = new OracleConnection(connectionString))
            {
                try
                {
                    OracleHelper.Connect(connection);
                    OracleTransaction transaction = connection.BeginTransaction();
                    OracleCommand command = connection.CreateCommand();
                    OracleParameter payloadParam = new OracleParameter("p_payload", OracleDbType.Clob, ParameterDirection.Output);

                    command.CommandText = $"{packageName}.{procedureName}";
                    command.CommandType = CommandType.StoredProcedure;

                    command.Parameters.Add("p_symbolic_queue_name", OracleDbType.Varchar2, symbolicQueueName, ParameterDirection.Input);
                    command.Parameters.Add(payloadParam);
                    command.Parameters.Add("p_wait_secs", OracleDbType.BinaryFloat, waitSeconds, ParameterDirection.Input);

                    command.ExecuteNonQuery();
                    transaction.Commit();

                    var payLoadClob = (OracleClob)payloadParam.Value;

                    return !payLoadClob.IsNull ? payLoadClob.Value : string.Empty;
                }
                catch (OracleException ex)
                {
                    if (ex.Number == 25228)
                    {
                        return null;
                    }
                    else
                    {
                        Console.WriteLine($"DequeuMessage has failed due to :  {ex.InnerException ?? ex}");
                        return null;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"DequeuMessage has failed due to :  {ex.InnerException ?? ex}");
                    return null;
                }
            }
        }

        public bool CreateEEPTrigger(string connectionString, string packageName, string procedureName, Dictionary<string, string> eepRecord)
        {
            using (OracleConnection connection = new OracleConnection(connectionString))
            {
                try
                {
                    OracleHelper.Connect(connection);
                    OracleTransaction transaction = connection.BeginTransaction();
                    OracleCommand command = connection.CreateCommand();
                    command.CommandText = $"{packageName}.{procedureName}";
                    command.CommandType = CommandType.StoredProcedure;
                    command.Parameters.Add("P_ENTITY_TYPE_ID", OracleDbType.Varchar2, eepRecord["ENTITY_TYPE_ID"], ParameterDirection.Input);
                    command.Parameters.Add("P_EVENT_TYPE_ID", OracleDbType.Double, Convert.ToDouble(eepRecord["EVENT_TYPE_ID"]), ParameterDirection.Input);
                    command.Parameters.Add("P_EVENT_ENTITY_ID", OracleDbType.Double, Convert.ToDouble(eepRecord["EVENT_ENTITY_ID"]), ParameterDirection.Input);
                    command.Parameters.Add("P_EVENT_ENTITY_ETID", OracleDbType.Varchar2, eepRecord["EVENT_ENTITY_ETID"], ParameterDirection.Input);
                    command.Parameters.Add("P_EVENT_ENTITY_ROW_STATE", OracleDbType.Char, eepRecord["EVENT_ENTITY_ROW_STATE"], ParameterDirection.Input);
                    command.Parameters.Add("P_EVENT_PARENT_ENTITY_ID", OracleDbType.Double, null, ParameterDirection.Input);
                    command.Parameters.Add("P_EVENT_PARENT_ENTITY_ETID", OracleDbType.Varchar2, null, ParameterDirection.Input);
                    command.Parameters.Add("P_EVENT_DATA", OracleDbType.Varchar2, eepRecord["EVENT_DATA"], ParameterDirection.Input);
                    command.ExecuteNonQuery();
                    transaction.Commit();
                    return true;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Stored Procedure has failed due to :  {ex.InnerException ?? ex}");
                    return false;
                }
            }
        }
    }
}
