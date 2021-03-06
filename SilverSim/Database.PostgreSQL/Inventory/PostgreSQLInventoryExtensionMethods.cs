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
using SilverSim.Types;
using SilverSim.Types.Asset;
using SilverSim.Types.Inventory;
using System.Collections.Generic;

namespace SilverSim.Database.PostgreSQL.Inventory
{
    public static class PostgreSQLInventoryExtensionMethods
    {
        public static InventoryFolder ToFolder(this NpgsqlDataReader reader) => new InventoryFolder(reader.GetUUID("ID"))
        {
            ParentFolderID = reader.GetUUID("ParentFolderID"),
            Name = (string)reader["Name"],
            DefaultType = reader.GetEnum<AssetType>("DefaultType"),
            Owner = reader.GetUGUI("OwnerID"),
            Version = (int)reader["Version"]
        };

        public static Dictionary<string, object> ToDictionary(this InventoryFolder folder) => new Dictionary<string, object>
        {
            ["ID"] = folder.ID,
            ["ParentFolderID"] = folder.ParentFolderID,
            ["Name"] = folder.Name,
            ["DefaultType"] = folder.DefaultType,
            ["OwnerID"] = folder.Owner.ID,
            ["Version"] = folder.Version
        };

        public static InventoryItem ToItem(this NpgsqlDataReader reader)
        {
            var item = new InventoryItem(reader.GetUUID("ID"))
            {
                ParentFolderID = reader.GetUUID("ParentFolderID"),
                Name = (string)reader["Name"],
                Description = (string)reader["Description"],
                InventoryType = reader.GetEnum<InventoryType>("InventoryType"),
                Flags = reader.GetEnum<InventoryFlags>("Flags"),
                CreationDate = reader.GetDate("CreationDate"),
                IsGroupOwned = (bool)reader["IsGroupOwned"],
                AssetID = reader.GetUUID("AssetID"),
                AssetType = reader.GetEnum<AssetType>("AssetType"),

                Owner = reader.GetUGUI("OwnerID"),
                LastOwner = reader.GetUGUI("LastOwnerID"),

                Creator = reader.GetUGUI("CreatorID"),
                Group = new UGI(reader.GetUUID("GroupID"))
            };
            item.Permissions.Base = reader.GetEnum<InventoryPermissionsMask>("BasePermissionsMask");
            item.Permissions.Current = reader.GetEnum<InventoryPermissionsMask>("CurrentPermissionsMask");
            item.Permissions.EveryOne = reader.GetEnum<InventoryPermissionsMask>("EveryOnePermissionsMask");
            item.Permissions.NextOwner = reader.GetEnum<InventoryPermissionsMask>("NextOwnerPermissionsMask");
            item.Permissions.Group = reader.GetEnum<InventoryPermissionsMask>("GroupPermissionsMask");
            item.SaleInfo.Price = (int)reader["SalePrice"];
            item.SaleInfo.Type = reader.GetEnum<InventoryItem.SaleInfoData.SaleType>("SaleType");
            item.SaleInfo.PermMask = reader.GetEnum<InventoryPermissionsMask>("SalePermissionsMask");

            return item;
        }

        public static Dictionary<string, object> ToDictionary(this InventoryItem item) => new Dictionary<string, object>
        {
            ["ID"] = item.ID,
            ["ParentFolderID"] = item.ParentFolderID,
            ["Name"] = item.Name,
            ["Description"] = item.Description,
            ["InventoryType"] = item.InventoryType,
            ["Flags"] = item.Flags,
            ["OwnerID"] = item.Owner.ID,
            ["CreatorID"] = item.Creator.ID,
            ["LastOwnerID"] = item.LastOwner.ID,
            ["CreationDate"] = item.CreationDate.DateTimeToUnixTime(),
            ["BasePermissionsMask"] = (uint)item.Permissions.Base,
            ["CurrentPermissionsMask"] = (uint)item.Permissions.Current,
            ["EveryOnePermissionsMask"] = (uint)item.Permissions.EveryOne,
            ["NextOwnerPermissionsMask"] = (uint)item.Permissions.NextOwner,
            ["GroupPermissionsMask"] = (uint)item.Permissions.Group,
            ["SalePrice"] = item.SaleInfo.Price,
            ["SaleType"] = item.SaleInfo.Type,
            ["GroupID"] = item.Group.ID,
            ["IsGroupOwned"] = item.IsGroupOwned,
            ["AssetID"] = item.AssetID,
            ["AssetType"] = item.AssetType
        };
    }
}
