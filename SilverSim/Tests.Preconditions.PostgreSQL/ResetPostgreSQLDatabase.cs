// SilverSim is distributed under the terms of the
// GNU Affero General Public License v3 with
// the following clarification and special exception.

// Linking this library statically or dynamically with other modules is
// making a combined work based on this library. Thus, the terms and
// conditions of the GNU Affero General Public License cover the whole
// combination.

// As a special exception, the copyright holders of this library give you
// permission to link this library with independent modules to produce an
// executable, regardless of the license terms of these independent
// modules, and to copy and distribute the resulting executable under
// terms of your choice, provided that you also meet, for each linked
// independent module, the terms and conditions of the license of that
// module. An independent module is a module which is not derived from
// or based on this library. If you modify this library, you may extend
// this exception to your version of the library, but you are not
// obligated to do so. If you do not wish to do so, delete this
// exception statement from your version.

using log4net;
using Npgsql;
using Nini.Config;
using SilverSim.Main.Common;
using SilverSim.ServiceInterfaces.Database;
using System.Collections.Generic;

namespace SilverSim.Tests.Preconditions.PostgreSQL
{
    [PluginName("ResetDatabase")]
    public class ResetPostgreSQLDatabase : IPlugin, IDBServiceInterface
    {
        private static readonly ILog m_Log = LogManager.GetLogger("POSTGRESQL DATABASE RESET");
        string m_ConnectionString;

        public ResetPostgreSQLDatabase(IConfig config)
        {
            NpgsqlConnectionStringBuilder sb = new NpgsqlConnectionStringBuilder();
            if (!(config.Contains("Server") && config.Contains("Username") && config.Contains("Password") && config.Contains("Database")))
            {
                if (!config.Contains("Server"))
                {
                    m_Log.FatalFormat("[POSTGRESQL CONFIG]: Parameter 'Server' missing in [{0}]", config.Name);
                }
                if (!config.Contains("Username"))
                {
                    m_Log.FatalFormat("[POSTGRESQL CONFIG]: Parameter 'Username' missing in [{0}]", config.Name);
                }
                if (!config.Contains("Password"))
                {
                    m_Log.FatalFormat("[POSTGRESQL CONFIG]: Parameter 'Password' missing in [{0}]", config.Name);
                }
                if (!config.Contains("Database"))
                {
                    m_Log.FatalFormat("[POSTGRESQL CONFIG]: Parameter 'Database' missing in [{0}]", config.Name);
                }
                throw new ConfigurationLoader.ConfigurationErrorException();
            }
            sb.Host = config.GetString("Server");
            sb.Username = config.GetString("Username");
            sb.Password = config.GetString("Password");
            sb.Database = config.GetString("Database");
            m_ConnectionString = sb.ToString();
        }

        public void Startup(ConfigurationLoader loader)
        {
        }

        public void VerifyConnection()
        {
            var tables = new List<string>();

            using (var connection = new NpgsqlConnection(m_ConnectionString))
            {
                connection.Open();
                m_Log.Info("Executing reset database");
                using(var cmd = new NpgsqlCommand("SELECT relname FROM pg_description " +
                        "JOIN pg_class ON pg_description.objoid = pg_class.oid " +
                        "JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid", connection))
                {
                    using(NpgsqlDataReader reader = cmd.ExecuteReader())
                    {
                        while(reader.Read())
                        {
                            tables.Add((string)reader.GetValue(0));
                        }
                    }
                }

                m_Log.InfoFormat("Deleting {0} tables", tables.Count);
                foreach (string table in tables)
                {
                    m_Log.InfoFormat("Deleting table {0}", table);
                    using (var cmd = new NpgsqlCommand(string.Format("DROP TABLE {0}", table), connection))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        public void ProcessMigrations()
        {
        }
    }
}
