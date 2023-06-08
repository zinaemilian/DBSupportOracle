using Microsoft.Win32;
using Oracle.ManagedDataAccess.Client;

namespace OracleHelpers.Helpers
{
    public static class OracleHelper
    {
        private static string _oraKeyValue;
        private static string _oraKeyName = "SOFTWARE\\Oracle\\KEY_OraClient19Home1";
        private static string _tnsPath = "\\network\\admin";

        private static string OralceHomePath()
        {
            RegistryKey baseKey = RegistryKey.OpenBaseKey(RegistryHive.LocalMachine, RegistryView.Registry64);
            RegistryKey key = baseKey.OpenSubKey(_oraKeyName, RegistryKeyPermissionCheck.ReadSubTree);
            if (key != null)
            {
                _oraKeyValue = key.GetValue("ORACLE_HOME").ToString();
            }
            return _oraKeyValue;
        }

        public static void Connect(OracleConnection connection)
        {
            connection.TnsAdmin = OralceHomePath() + _tnsPath;
            connection.Open();
        }

        public static void Connect(OracleConnection connection, string oracleTnsPath)
        {
            connection.TnsAdmin = oracleTnsPath;
            connection.Open();
        }
    }
}
