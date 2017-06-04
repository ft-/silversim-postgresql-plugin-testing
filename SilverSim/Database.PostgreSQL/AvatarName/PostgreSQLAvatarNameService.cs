﻿// SilverSim is distributed under the terms of the
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
using Nini.Config;
using Npgsql;
using SilverSim.Database.PostgreSQL._Migration;
using SilverSim.Main.Common;
using SilverSim.ServiceInterfaces.AvatarName;
using SilverSim.ServiceInterfaces.Database;
using SilverSim.Types;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using PostgreSQLInsertException = SilverSim.Database.PostgreSQL.PostgreSQLUtilities.PostgreSQLInsertException;

namespace SilverSim.Database.PostgreSQL.AvatarName
{
    [Description("PostgreSQL AvatarName Backend")]
    [PluginName("AvatarNames")]
    public class PostgreSQLAvatarNameService: AvatarNameServiceInterface, IDBServiceInterface, IPlugin
    {
        private readonly string m_ConnectionString;
        private static readonly ILog m_Log = LogManager.GetLogger("POSTGRESQL AVATAR NAMES SERVICE");

        #region Constructor
        public PostgreSQLAvatarNameService(IConfig ownSection)
        {
            m_ConnectionString = PostgreSQLUtilities.BuildConnectionString(ownSection, m_Log);
        }

        public void Startup(ConfigurationLoader loader)
        {
            /* intentionally left empty */
        }
        #endregion

        #region Accessors
        public override bool TryGetValue(string firstName, string lastName, out UUI uui)
        {
            using (var connection = new NpgsqlConnection(m_ConnectionString))
            {
                connection.Open();

                using (var cmd = new NpgsqlCommand("SELECT * FROM avatarnames WHERE FirstName = @firstName AND LastName = @lastName", connection))
                {
                    cmd.Parameters.AddParameter("@firstName", firstName);
                    cmd.Parameters.AddParameter("@lastName", lastName);
                    using (NpgsqlDataReader dbreader = cmd.ExecuteReader())
                    {
                        if (!dbreader.Read())
                        {
                            uui = default(UUI);
                            return false;
                        }
                        uui = ToUUI(dbreader);
                        return true;
                    }
                }
            }
        }

        public override UUI this[string firstName, string lastName]
        {
            get
            {
                UUI uui;
                if (!TryGetValue(firstName, lastName, out uui))
                {
                    throw new KeyNotFoundException();
                }
                return uui;
            }
        }

        public override bool TryGetValue(UUID key, out UUI uui)
        {
            using (var connection = new NpgsqlConnection(m_ConnectionString))
            {
                connection.Open();

                using (var cmd = new NpgsqlCommand("SELECT * FROM avatarnames WHERE AvatarID = @avatarid", connection))
                {
                    cmd.Parameters.AddParameter("@avatarid", key);
                    using (NpgsqlDataReader dbreader = cmd.ExecuteReader())
                    {
                        if (!dbreader.Read())
                        {
                            uui = default(UUI);
                            return false;
                        }
                        uui = ToUUI(dbreader);
                        return true;
                    }
                }
            }
        }

        public override UUI this[UUID key]
        {
            get
            {
                UUI uui;
                if (!TryGetValue(key, out uui))
                {
                    throw new KeyNotFoundException();
                }
                return uui;
            }
        }
        #endregion

        public override void Store(UUI value)
        {
            if (value.IsAuthoritative) /* do not store non-authoritative entries */
            {
                var data = new Dictionary<string, object>
                {
                    ["AvatarID"] = value.ID,
                    ["HomeURI"] = value.HomeURI,
                    ["FirstName"] = value.FirstName,
                    ["LastName"] = value.LastName
                };
                using (var connection = new NpgsqlConnection(m_ConnectionString))
                {
                    connection.Open();

                    if(connection.HasOnConflict())
                    {
                        using (var cmd = new NpgsqlCommand("INSERT INTO avatarnames (\"AvatarID\", \"HomeURI\", \"FirstName\", \"LastName\") VALUES (@avatarid, @homeuri, @firstname, @lastname) ON CONFLICT (AvatarID) DO UPDATE SET \"HomeURI\"=@homeuri, \"FirstName\"=@firstname,\"LastName\"=@lastname", connection))
                        {
                            cmd.Parameters.AddParameter("@avatarid", (Guid)value.ID);
                            cmd.Parameters.AddParameter("@homeuri", value.HomeURI);
                            cmd.Parameters.AddParameter("@firstname", value.FirstName);
                            cmd.Parameters.AddParameter("@lastname", value.LastName);
                            if (cmd.ExecuteNonQuery() < 1)
                            {
                                throw new PostgreSQLInsertException();
                            }
                        }
                    }
                    else
                    {
                        string cmdstring = "UPDATE avatarnames SET \"HomeURI\"=@homeuri, \"FirstName\"=@firstname,\"LastName\"=@lastname WHERE \"AvatarID\" = @avatarid;";
                        cmdstring += "INSERT INTO avatarnames (\"AvatarID\", \"HomeURI\", \"FirstName\", \"LastName\") SELECT @avatarid, @homeuri, @firstname, @lastname WHERE NOT EXISTS (SELECT 1 FROM avatarnames WHERE \"AvatarID\"=@avatarid);";
                        connection.InsideTransaction(() =>
                        {
                            using (var cmd = new NpgsqlCommand(cmdstring, connection))
                            {
                                cmd.Parameters.AddParameter("@avatarid", (Guid)value.ID);
                                cmd.Parameters.AddParameter("@homeuri", value.HomeURI);
                                cmd.Parameters.AddParameter("@firstname", value.FirstName);
                                cmd.Parameters.AddParameter("@lastname", value.LastName);
                                if (cmd.ExecuteNonQuery() < 1)
                                {
                                    throw new PostgreSQLInsertException();
                                }
                            }
                        });
                    }
                }
            }
        }

        public override bool Remove(UUID key)
        {
            using (var connection = new NpgsqlConnection(m_ConnectionString))
            {
                connection.Open();

                using (var cmd = new NpgsqlCommand("DELETE FROM avatarnames WHERE \"AvatarID\" = @id", connection))
                {
                    cmd.Parameters.AddParameter("@id", key);
                    return cmd.ExecuteNonQuery() == 1;
                }
            }
        }

        public override List<UUI> Search(string[] names)
        {
            if (names.Length < 1 || names.Length > 2)
            {
                return new List<UUI>();
            }

            if (names.Length == 1)
            {
                using (var connection = new NpgsqlConnection(m_ConnectionString))
                {
                    connection.Open();

                    using (var cmd = new NpgsqlCommand("SELECT * FROM avatarnames WHERE position(@name in FirstName) > 0 OR position(@name in LastName) > 0", connection))
                    {
                        cmd.Parameters.AddParameter("@name", names[0]);

                        return GetSearchResults(cmd);
                    }
                }
            }
            else
            {
                using (var connection = new NpgsqlConnection(m_ConnectionString))
                {
                    connection.Open();

                    using (var cmd = new NpgsqlCommand("SELECT * FROM avatarnames WHERE position(@firstname in FirstName) > 0 AND position(@lastname in LastName) > 0", connection))
                    {
                        cmd.Parameters.AddParameter("@firstname", names[0]);
                        cmd.Parameters.AddParameter("@lastname", names[1]);

                        return GetSearchResults(cmd);
                    }
                }
            }
        }

        private List<UUI> GetSearchResults(NpgsqlCommand cmd)
        {
            var results = new List<UUI>();
            using (NpgsqlDataReader dbreader = cmd.ExecuteReader())
            {
                while (dbreader.Read())
                {
                    results.Add(ToUUI(dbreader));
                }
                return results;
            }
        }

        private static UUI ToUUI(NpgsqlDataReader dbreader) => new UUI()
        {
            ID = dbreader.GetUUID("AvatarID"),
            HomeURI = dbreader.GetUri("HomeURI"),
            FirstName = (string)dbreader["FirstName"],
            LastName = (string)dbreader["LastName"],
            IsAuthoritative = true
        };

        public void VerifyConnection()
        {
            using (var connection = new NpgsqlConnection(m_ConnectionString))
            {
                connection.Open();
            }
        }

        public void ProcessMigrations()
        {
            using (var conn = new NpgsqlConnection(m_ConnectionString))
            {
                conn.Open();
                conn.MigrateTables(Migrations, m_Log);
            }
        }

        private static readonly IMigrationElement[] Migrations = new IMigrationElement[]
        {
            new SqlTable("avatarnames"),
            new AddColumn<UUID>("AvatarID") { IsNullAllowed = false, Default = UUID.Zero },
            new AddColumn<string>("HomeURI") { Cardinality = 255 },
            new AddColumn<string>("FirstName") { Cardinality = 255 },
            new AddColumn<string>("LastName") { Cardinality = 255 },
            new PrimaryKeyInfo("AvatarID", "HomeURI")
        };
    }
}
