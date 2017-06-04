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

using Npgsql;
using SilverSim.Scene.ServiceInterfaces.SimulationData;
using SilverSim.Scene.Types.Scene;
using SilverSim.Types;
using System.Collections.Generic;

namespace SilverSim.Database.PostgreSQL.SimulationData
{
    public sealed partial class PostgreSQLSimulationDataStorage : ISimulationDataRegionSettingsStorageInterface
    {
        private RegionSettings ToRegionSettings(NpgsqlDataReader reader) => new RegionSettings()
        {
            BlockTerraform = (bool)reader["BlockTerraform"],
            BlockFly = (bool)reader["BlockFly"],
            AllowDamage = (bool)reader["AllowDamage"],
            RestrictPushing = (bool)reader["RestrictPushing"],
            AllowLandResell = (bool)reader["AllowLandResell"],
            AllowLandJoinDivide = (bool)reader["AllowLandJoinDivide"],
            BlockShowInSearch = (bool)reader["BlockShowInSearch"],
            AgentLimit = (int)reader["AgentLimit"],
            ObjectBonus = (double)reader["ObjectBonus"],
            DisableScripts = (bool)reader["DisableScripts"],
            DisableCollisions = (bool)reader["DisableCollisions"],
            BlockFlyOver = (bool)reader["BlockFlyOver"],
            Sandbox = (bool)reader["Sandbox"],
            TerrainTexture1 = reader.GetUUID("TerrainTexture1"),
            TerrainTexture2 = reader.GetUUID("TerrainTexture2"),
            TerrainTexture3 = reader.GetUUID("TerrainTexture3"),
            TerrainTexture4 = reader.GetUUID("TerrainTexture4"),
            TelehubObject = reader.GetUUID("TelehubObject"),
            Elevation1NW = (double)reader["Elevation1NW"],
            Elevation2NW = (double)reader["Elevation2NW"],
            Elevation1NE = (double)reader["Elevation1NE"],
            Elevation2NE = (double)reader["Elevation2NE"],
            Elevation1SE = (double)reader["Elevation1SE"],
            Elevation2SE = (double)reader["Elevation2SE"],
            Elevation1SW = (double)reader["Elevation1SW"],
            Elevation2SW = (double)reader["Elevation2SW"],
            WaterHeight = (double)reader["WaterHeight"],
            TerrainRaiseLimit = (double)reader["TerrainRaiseLimit"],
            TerrainLowerLimit = (double)reader["TerrainLowerLimit"],
            SunPosition = (double)reader["SunPosition"],
            IsSunFixed = (bool)reader["IsSunFixed"],
            UseEstateSun = (bool)reader["UseEstateSun"],
            BlockDwell = (bool)reader["BlockDwell"],
            ResetHomeOnTeleport = (bool)reader["ResetHomeOnTeleport"],
            AllowLandmark = (bool)reader["AllowLandmark"],
            AllowDirectTeleport = (bool)reader["AllowDirectTeleport"]
        };

        RegionSettings ISimulationDataRegionSettingsStorageInterface.this[UUID regionID]
        {
            get
            {
                RegionSettings settings;
                if (!RegionSettings.TryGetValue(regionID, out settings))
                {
                    throw new KeyNotFoundException();
                }
                return settings;
            }
            set
            {
                using (var conn = new NpgsqlConnection(m_ConnectionString))
                {
                    conn.Open();
                    var data = new Dictionary<string, object>
                    {
                        ["RegionID"] = regionID,
                        ["BlockTerraform"] = value.BlockTerraform,
                        ["BlockFly"] = value.BlockFly,
                        ["AllowDamage"] = value.AllowDamage,
                        ["RestrictPushing"] = value.RestrictPushing,
                        ["AllowLandResell"] = value.AllowLandResell,
                        ["AllowLandJoinDivide"] = value.AllowLandJoinDivide,
                        ["BlockShowInSearch"] = value.BlockShowInSearch,
                        ["AgentLimit"] = value.AgentLimit,
                        ["ObjectBonus"] = value.ObjectBonus,
                        ["DisableScripts"] = value.DisableScripts,
                        ["DisableCollisions"] = value.DisableCollisions,
                        ["BlockFlyOver"] = value.BlockFlyOver,
                        ["Sandbox"] = value.Sandbox,
                        ["TerrainTexture1"] = value.TerrainTexture1,
                        ["TerrainTexture2"] = value.TerrainTexture2,
                        ["TerrainTexture3"] = value.TerrainTexture3,
                        ["TerrainTexture4"] = value.TerrainTexture4,
                        ["TelehubObject"] = value.TelehubObject,
                        ["Elevation1NW"] = value.Elevation1NW,
                        ["Elevation2NW"] = value.Elevation2NW,
                        ["Elevation1NE"] = value.Elevation1NE,
                        ["Elevation2NE"] = value.Elevation2NE,
                        ["Elevation1SE"] = value.Elevation1SE,
                        ["Elevation2SE"] = value.Elevation2SE,
                        ["Elevation1SW"] = value.Elevation1SW,
                        ["Elevation2SW"] = value.Elevation2SW,
                        ["WaterHeight"] = value.WaterHeight,
                        ["TerrainRaiseLimit"] = value.TerrainRaiseLimit,
                        ["TerrainLowerLimit"] = value.TerrainLowerLimit,
                        ["SunPosition"] = value.SunPosition,
                        ["IsSunFixed"] = value.IsSunFixed,
                        ["UseEstateSun"] = value.UseEstateSun,
                        ["BlockDwell"] = value.BlockDwell,
                        ["ResetHomeOnTeleport"] = value.ResetHomeOnTeleport,
                        ["AllowLandmark"] = value.AllowLandmark,
                        ["AllowDirectTeleport"] = value.AllowDirectTeleport
                    };
                    conn.ReplaceInto("regionsettings", data, new string[] { "RegionID" }, m_EnableOnConflict);
                }
            }
        }

        bool ISimulationDataRegionSettingsStorageInterface.TryGetValue(UUID regionID, out RegionSettings settings)
        {
            using (var conn = new NpgsqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new NpgsqlCommand("SELECT * FROM regionsettings WHERE \"RegionID\" = @regionid", conn))
                {
                    cmd.Parameters.AddWithValue("@regionid", regionID);
                    using (NpgsqlDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            settings = ToRegionSettings(reader);
                            return true;
                        }
                    }
                }
            }
            settings = null;
            return false;
        }

        bool ISimulationDataRegionSettingsStorageInterface.ContainsKey(UUID regionID)
        {
            using (var conn = new NpgsqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new NpgsqlCommand("SELECT \"RegionID\" FROM regionsettings WHERE \"RegionID\" = @regionid", conn))
                {
                    cmd.Parameters.AddParameter("@regionid", regionID);
                    using (NpgsqlDataReader reader = cmd.ExecuteReader())
                    {
                        return reader.Read();
                    }
                }
            }
        }

        bool ISimulationDataRegionSettingsStorageInterface.Remove(UUID regionID)
        {
            using (var conn = new NpgsqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new NpgsqlCommand("DELETE FROM regionsettings WHERE \"RegionID\" = @regionid", conn))
                {
                    cmd.Parameters.AddParameter("@regionid", regionID);
                    return cmd.ExecuteNonQuery() > 0;
                }
            }
        }
    }
}
