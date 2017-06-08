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

using Npgsql;
using SilverSim.Scene.ServiceInterfaces.SimulationData;
using SilverSim.Scene.Types.Object;
using SilverSim.Types;
using SilverSim.Types.Agent;
using SilverSim.Types.Asset;
using SilverSim.Types.Inventory;
using SilverSim.Types.Primitive;
using SilverSim.Types.Script;
using SilverSim.Types.StructuredData.Llsd;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace SilverSim.Database.PostgreSQL.SimulationData
{
    public sealed partial class PostgreSQLSimulationDataStorage : ISimulationDataObjectStorageInterface
    {
        #region Objects and Prims within a region by UUID
        List<UUID> ISimulationDataObjectStorageInterface.ObjectsInRegion(UUID key)
        {
            var objects = new List<UUID>();

            using (var connection = new NpgsqlConnection(m_ConnectionString))
            {
                connection.Open();
                using (var cmd = new NpgsqlCommand("SELECT \"ID\" FROM objects WHERE \"RegionID\" = '" + key.ToString() + "'", connection))
                {
                    using (NpgsqlDataReader dbReader = cmd.ExecuteReader())
                    {
                        while (dbReader.Read())
                        {
                            objects.Add(dbReader.GetUUID("ID"));
                        }
                    }
                }
            }
            return objects;
        }

        List<UUID> ISimulationDataObjectStorageInterface.PrimitivesInRegion(UUID key)
        {
            var objects = new List<UUID>();

            using (var connection = new NpgsqlConnection(m_ConnectionString))
            {
                connection.Open();
                using (var cmd = new NpgsqlCommand("SELECT \"ID\" FROM prims WHERE \"RegionID\" = '" + key.ToString() + "'", connection))
                {
                    using (NpgsqlDataReader dbReader = cmd.ExecuteReader())
                    {
                        while (dbReader.Read())
                        {
                            objects.Add(dbReader.GetUUID("ID"));
                        }
                    }
                }
            }
            return objects;
        }
        #endregion

        #region helpers
        private ObjectGroup ObjectGroupFromDbReader(NpgsqlDataReader dbReader) => new ObjectGroup()
        {
            IsTempOnRez = (bool)dbReader["IsTempOnRez"],
            Owner = dbReader.GetUUI("Owner"),
            LastOwner = dbReader.GetUUI("LastOwner"),
            Group = dbReader.GetUGI("Group"),
            SaleType = dbReader.GetEnum<InventoryItem.SaleInfoData.SaleType>("SaleType"),
            SalePrice = (int)dbReader["SalePrice"],
            PayPrice0 = (int)dbReader["PayPrice0"],
            PayPrice1 = (int)dbReader["PayPrice1"],
            PayPrice2 = (int)dbReader["PayPrice2"],
            PayPrice3 = (int)dbReader["PayPrice3"],
            PayPrice4 = (int)dbReader["PayPrice4"],
            AttachedPos = dbReader.GetVector3("AttachedPos"),
            AttachPoint = dbReader.GetEnum<AttachmentPoint>("AttachPoint"),
            IsIncludedInSearch = (bool)dbReader["IsIncludedInSearch"],
            RezzingObjectID = dbReader.GetUUID("RezzingObjectID")
        };

        private ObjectPart ObjectPartFromDbReader(NpgsqlDataReader dbReader)
        {
            var objpart = new ObjectPart()
            {
                ID = dbReader.GetUUID("ID"),
                LoadedLinkNumber = (int)dbReader["LinkNumber"],
                Position = dbReader.GetVector3("Position"),
                Rotation = dbReader.GetQuaternion("Rotation"),
                SitText = (string)dbReader["SitText"],
                TouchText = (string)dbReader["TouchText"],
                Name = (string)dbReader["Name"],
                Description = (string)dbReader["Description"],
                SitTargetOffset = dbReader.GetVector3("SitTargetOffset"),
                SitTargetOrientation = dbReader.GetQuaternion("SitTargetOrientation"),
                Creator = dbReader.GetUUI("Creator"),
                CreationDate = dbReader.GetDate("CreationDate"),
                Flags = dbReader.GetEnum<PrimitiveFlags>("Flags"),

                CameraAtOffset = dbReader.GetVector3("CameraAtOffset"),
                CameraEyeOffset = dbReader.GetVector3("CameraEyeOffset"),

                PhysicsShapeType = dbReader.GetEnum<PrimitivePhysicsShapeType>("PhysicsShapeType"),
                PathfindingType = dbReader.GetEnum<PathfindingType>("PathfindingType"),
                Material = dbReader.GetEnum<PrimitiveMaterial>("Material"),
                Size = dbReader.GetVector3("Size"),
                Slice = dbReader.GetVector3("Slice"),

                MediaURL = (string)dbReader["MediaURL"],

                AngularVelocity = dbReader.GetVector3("AngularVelocity"),
                PointLight = new ObjectPart.PointLightParam()
                {
                    Serialization = dbReader.GetBytes("LightData")
                },
                Text = new ObjectPart.TextParam()
                {
                    Serialization = dbReader.GetBytes("HoverTextData")
                },
                Flexible = new ObjectPart.FlexibleParam()
                {
                    Serialization = dbReader.GetBytes("FlexibleData")
                },
                Sound = new ObjectPart.SoundParam()
                {
                    Serialization = dbReader.GetBytes("LoopedSoundData")
                },
                CollisionSound = new ObjectPart.CollisionSoundParam()
                {
                    Serialization = dbReader.GetBytes("ImpactSoundData")
                },
                Shape = new ObjectPart.PrimitiveShape()
                {
                    Serialization = dbReader.GetBytes("PrimitiveShapeData")
                },
                ParticleSystemBytes = dbReader.GetBytes("ParticleSystem"),
                TextureEntryBytes = dbReader.GetBytes("TextureEntryBytes"),
                TextureAnimationBytes = dbReader.GetBytes("TextureAnimationBytes"),

                ScriptAccessPin = (int)dbReader["ScriptAccessPin"],
                ForceMouselook = (bool)dbReader["ForceMouselook"],
                BaseMask = dbReader.GetEnum<InventoryPermissionsMask>("BasePermissions"),
                OwnerMask = dbReader.GetEnum<InventoryPermissionsMask>("CurrentPermissions"),
                EveryoneMask = dbReader.GetEnum<InventoryPermissionsMask>("EveryOnePermissions"),
                GroupMask = dbReader.GetEnum<InventoryPermissionsMask>("GroupPermissions"),
                NextOwnerMask = dbReader.GetEnum<InventoryPermissionsMask>("NextOwnerPermissions"),
                ClickAction = dbReader.GetEnum<ClickActionType>("ClickAction"),
                PassCollisionMode = dbReader.GetEnum<PassEventMode>("PassCollisionMode"),
                PassTouchMode = dbReader.GetEnum<PassEventMode>("PassTouchMode"),
                Velocity = dbReader.GetVector3("Velocity"),
                IsSoundQueueing = (bool)dbReader["IsSoundQueueing"],
                IsAllowedDrop = (bool)dbReader["IsAllowedDrop"],
                PhysicsDensity = (double)dbReader["PhysicsDensity"],
                PhysicsFriction = (double)dbReader["PhysicsFriction"],
                PhysicsRestitution = (double)dbReader["PhysicsRestitution"],
                PhysicsGravityMultiplier = (double)dbReader["PhysicsGravityMultiplier"],

                IsVolumeDetect = (bool)dbReader["IsVolumeDetect"],
                IsPhantom = (bool)dbReader["IsPhantom"],
                IsPhysics = (bool)dbReader["IsPhysics"],
                IsRotateZEnabled = (bool)dbReader["IsRotateZEnabled"],
                IsRotateXEnabled = (bool)dbReader["IsRotateXEnabled"],
                IsRotateYEnabled = (bool)dbReader["IsRotateYEnabled"],
            };

            using (var ms = new MemoryStream(dbReader.GetBytes("DynAttrs")))
            {
                foreach (KeyValuePair<string, IValue> kvp in (Map)LlsdBinary.Deserialize(ms))
                {
                    objpart.DynAttrs.Add(kvp.Key, kvp.Value);
                }
            }

            return objpart;
        }

        private ObjectPartInventoryItem ObjectPartInventoryItemFromDbReader(NpgsqlDataReader dbReader)
        {
            var item = new ObjectPartInventoryItem()
            {
                AssetID = dbReader.GetUUID("AssetID"),
                AssetType = dbReader.GetEnum<AssetType>("AssetType"),
                CreationDate = dbReader.GetDate("CreationDate"),
                Creator = dbReader.GetUUI("Creator"),
                Description = (string)dbReader["Description"],
                Flags = dbReader.GetEnum<InventoryFlags>("Flags"),
                Group = dbReader.GetUGI("Group"),
                IsGroupOwned = (bool)dbReader["GroupOwned"],
                ID = dbReader.GetUUID("InventoryID"),
                InventoryType = dbReader.GetEnum<InventoryType>("InventoryType"),
                LastOwner = dbReader.GetUUI("LastOwner"),
                Name = (string)dbReader["Name"],
                Owner = dbReader.GetUUI("Owner"),
                ParentFolderID = dbReader.GetUUID("ParentFolderID"),
                NextOwnerAssetID = dbReader.GetUUID("NextOwnerAssetID")
            };
            item.Permissions.Base = dbReader.GetEnum<InventoryPermissionsMask>("BasePermissions");
            item.Permissions.Current = dbReader.GetEnum<InventoryPermissionsMask>("CurrentPermissions");
            item.Permissions.EveryOne = dbReader.GetEnum<InventoryPermissionsMask>("EveryOnePermissions");
            item.Permissions.Group = dbReader.GetEnum<InventoryPermissionsMask>("GroupPermissions");
            item.Permissions.NextOwner = dbReader.GetEnum<InventoryPermissionsMask>("NextOwnerPermissions");
            item.SaleInfo.Type = dbReader.GetEnum<InventoryItem.SaleInfoData.SaleType>("SaleType");
            item.SaleInfo.Price = (int)dbReader["SalePrice"];
            item.SaleInfo.PermMask = dbReader.GetEnum<InventoryPermissionsMask>("SalePermMask");
            var grantinfo = new ObjectPartInventoryItem.PermsGranterInfo();
            if (((string)dbReader["PermsGranter"]).Length != 0)
            {
                try
                {
                    grantinfo.PermsGranter = dbReader.GetUUI("PermsGranter");
                }
                catch
                {
                    /* no action required */
                }
            }
            grantinfo.PermsMask = dbReader.GetEnum<ScriptPermissions>("PermsMask");

            return item;
        }
        #endregion

        #region Load all object groups of a single region
        List<ObjectGroup> ISimulationDataObjectStorageInterface.this[UUID regionID]
        {
            get
            {
                var objGroups = new Dictionary<UUID, ObjectGroup>();
                var originalAssetIDs = new Dictionary<UUID, UUID>();
                var nextOwnerAssetIDs = new Dictionary<UUID, UUID>();
                var objGroupParts = new Dictionary<UUID, SortedDictionary<int, ObjectPart>>();
                var objPartIDs = new List<UUID>();
                var objParts = new Dictionary<UUID, ObjectPart>();
                var orphanedPrims = new List<UUID>();
                var orphanedPrimInventories = new List<KeyValuePair<UUID, UUID>>();

                using (var connection = new NpgsqlConnection(m_ConnectionString))
                {
                    connection.Open();
                    UUID objgroupID = UUID.Zero;
                    m_Log.InfoFormat("Loading object groups for region ID {0}", regionID);

                    using (var cmd = new NpgsqlCommand("SELECT * FROM objects WHERE \"RegionID\" = '" + regionID.ToString() + "'", connection))
                    {
                        cmd.CommandTimeout = 3600;
                        using (NpgsqlDataReader dbReader = cmd.ExecuteReader())
                        {
                            while (dbReader.Read())
                            {
                                try
                                {
                                    objgroupID = dbReader.GetUUID("ID");
                                    originalAssetIDs[objgroupID] = dbReader.GetUUID("OriginalAssetID");
                                    nextOwnerAssetIDs[objgroupID] = dbReader.GetUUID("NextOwnerAssetID");
                                    objGroups[objgroupID] = ObjectGroupFromDbReader(dbReader);
                                }
                                catch (Exception e)
                                {
                                    m_Log.WarnFormat("Failed to load object {0}: {1}\n{2}", objgroupID, e.Message, e.StackTrace);
                                    objGroups.Remove(objgroupID);
                                }
                            }
                        }
                    }

                    m_Log.InfoFormat("Loading prims for region ID {0}", regionID);
                    int primcount = 0;
                    using (var cmd = new NpgsqlCommand("SELECT * FROM prims WHERE \"RegionID\" = '" + regionID.ToString() + "'", connection))
                    {
                        cmd.CommandTimeout = 3600;
                        using (NpgsqlDataReader dbReader = cmd.ExecuteReader())
                        {
                            while (dbReader.Read())
                            {
                                UUID rootPartID = dbReader.GetUUID("RootPartID");
                                if (objGroups.ContainsKey(rootPartID))
                                {
                                    if (!objGroupParts.ContainsKey(rootPartID))
                                    {
                                        objGroupParts.Add(rootPartID, new SortedDictionary<int, ObjectPart>());
                                    }

                                    ObjectPart objpart = ObjectPartFromDbReader(dbReader);

                                    objGroupParts[rootPartID].Add(objpart.LoadedLinkNumber, objpart);
                                    objPartIDs.Add(objpart.ID);
                                    objParts[objpart.ID] = objpart;
                                    if ((++primcount) % 5000 == 0)
                                    {
                                        m_Log.InfoFormat("Loading prims for region ID {0} - {1} loaded", regionID, primcount);
                                    }
                                }
                                else
                                {
                                    m_Log.WarnFormat("deleting orphan prim in region ID {0}: {1}", regionID, dbReader.GetUUID("ID"));
                                    orphanedPrims.Add(dbReader.GetUUID("ID"));
                                }
                            }
                        }
                    }
                    m_Log.InfoFormat("Loaded prims for region ID {0} - {1} loaded", regionID, primcount);

                    int primitemcount = 0;
                    m_Log.InfoFormat("Loading prim inventories for region ID {0}", regionID);
                    using (var cmd = new NpgsqlCommand("SELECT * FROM primitems WHERE \"RegionID\" = '" + regionID.ToString() + "'", connection))
                    {
                        cmd.CommandTimeout = 3600;
                        using (NpgsqlDataReader dbReader = cmd.ExecuteReader())
                        {
                            while (dbReader.Read())
                            {
                                UUID partID = dbReader.GetUUID("PrimID");
                                ObjectPart part;
                                if (objParts.TryGetValue(partID, out part))
                                {
                                    ObjectPartInventoryItem item = ObjectPartInventoryItemFromDbReader(dbReader);

                                    part.Inventory.Add(item.ID, item.Name, item);
                                    if ((++primitemcount) % 5000 == 0)
                                    {
                                        m_Log.InfoFormat("Loading prim inventories for region ID {0} - {1} loaded", regionID, primitemcount);
                                    }
                                }
                                else
                                {
                                    m_Log.WarnFormat("deleting orphan prim in region ID {0}: {1}", regionID, dbReader.GetUUID("ID"));
                                    orphanedPrimInventories.Add(new KeyValuePair<UUID, UUID>(dbReader.GetUUID("PrimID"), dbReader.GetUUID("ID")));
                                }
                            }
                        }
                    }
                    m_Log.InfoFormat("Loaded prim inventories for region ID {0} - {1} loaded", regionID, primitemcount);
                }

                var removeObjGroups = new List<UUID>();
                foreach (KeyValuePair<UUID, ObjectGroup> kvp in objGroups)
                {
                    if (!objGroupParts.ContainsKey(kvp.Key))
                    {
                        removeObjGroups.Add(kvp.Key);
                    }
                    else
                    {
                        foreach (ObjectPart objpart in objGroupParts[kvp.Key].Values)
                        {
                            kvp.Value.Add(objpart.LoadedLinkNumber, objpart.ID, objpart);
                        }

                        try
                        {
                            kvp.Value.OriginalAssetID = originalAssetIDs[kvp.Value.ID];
                            kvp.Value.NextOwnerAssetID = nextOwnerAssetIDs[kvp.Value.ID];
                            kvp.Value.FinalizeObject();
                        }
                        catch
                        {
                            m_Log.WarnFormat("deleting orphan object in region ID {0}: {1}", regionID, kvp.Key);
                            removeObjGroups.Add(kvp.Key);
                        }
                    }
                }

                foreach (UUID objid in removeObjGroups)
                {
                    objGroups.Remove(objid);
                }

                for (int idx = 0; idx < removeObjGroups.Count; idx += 256)
                {
                    int elemcnt = Math.Min(removeObjGroups.Count - idx, 256);
                    string sqlcmd = "DELETE FROM objects WHERE \"RegionID\" = '" + regionID.ToString() + "' AND \"ID\" IN (" +
                        string.Join(",", from id in removeObjGroups.GetRange(idx, elemcnt) select "'" + id.ToString() + "'") +
                        ")";
                    using (var conn = new NpgsqlConnection(m_ConnectionString))
                    {
                        conn.Open();
                        using (var cmd = new NpgsqlCommand(sqlcmd, conn))
                        {
                            cmd.ExecuteNonQuery();
                        }
                    }
                }

                for (int idx = 0; idx < orphanedPrims.Count; idx += 256)
                {
                    int elemcnt = Math.Min(orphanedPrims.Count - idx, 256);
                    string sqlcmd = "DELETE FROM prims WHERE \"RegionID\" = '" + regionID.ToString() + "' AND \"ID\" IN (" +
                        string.Join(",", from id in orphanedPrims.GetRange(idx, elemcnt) select "'" + id.ToString() + "'") +
                        ")";
                    using (var conn = new NpgsqlConnection(m_ConnectionString))
                    {
                        conn.Open();
                        using (var cmd = new NpgsqlCommand(sqlcmd, conn))
                        {
                            cmd.ExecuteNonQuery();
                        }
                    }
                }
                for (int idx = 0; idx < orphanedPrimInventories.Count; idx += 256)
                {
                    int elemcnt = Math.Min(orphanedPrimInventories.Count - idx, 256);
                    string sqlcmd = "DELETE FROM primitems WHERE \"RegionID\" = '" + regionID.ToString() + "' AND (" +
                        string.Join(" OR ", from id in orphanedPrimInventories.GetRange(idx, elemcnt)
                                            select string.Format("\"PrimID\" = '{0}' AND \"ID\" = '{1}'", id.Key.ToString(), id.Value.ToString()));
                    using (var conn = new NpgsqlConnection(m_ConnectionString))
                    {
                        conn.Open();
                        using (var cmd = new NpgsqlCommand(sqlcmd, conn))
                        {
                            cmd.ExecuteNonQuery();
                        }
                    }
                }

                return new List<ObjectGroup>(objGroups.Values);
            }
        }
        #endregion
    }
}