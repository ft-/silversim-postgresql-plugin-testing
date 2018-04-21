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
using SilverSim.Types;
using SilverSim.Types.Asset;
using SilverSim.Types.Groups;
using System;

namespace SilverSim.Database.PostgreSQL.Groups
{
    public static class PostgreSQLGroupsServiceExtensionMethods
    {
        public static GroupInfo ToGroupInfo(this NpgsqlDataReader reader, string memberCount = "MemberCount")
        {
            var info = new GroupInfo();
            info.ID.ID = reader.GetUUID("GroupID");
            string uri = (string)reader["Location"];
            if (!string.IsNullOrEmpty(uri))
            {
                info.ID.HomeURI = new Uri(uri, UriKind.Absolute);
            }
            info.ID.GroupName = (string)reader["Name"];
            info.Charter = (string)reader["Charter"];
            info.InsigniaID = reader.GetUUID("InsigniaID");
            info.Founder.ID = reader.GetUUID("FounderID");
            info.MembershipFee = (int)reader["MembershipFee"];
            info.IsOpenEnrollment = (bool)reader["OpenEnrollment"];
            info.IsShownInList = (bool)reader["ShowInList"];
            info.IsAllowPublish = (bool)reader["AllowPublish"];
            info.IsMaturePublish = (bool)reader["MaturePublish"];
            info.OwnerRoleID = reader.GetUUID("OwnerRoleID");
            info.MemberCount = (int)(long)reader[memberCount];
            info.RoleCount = (int)(long)reader["RoleCount"];

            return info;
        }

        public static GroupRole ToGroupRole(this NpgsqlDataReader reader, string prefix = "")
        {
            var role = new GroupRole
            {
                Group = new UGI(reader.GetUUID("GroupID")),
                ID = reader.GetUUID("RoleID"),
                Name = (string)reader[prefix + "Name"],
                Description = (string)reader[prefix + "Description"],
                Title = (string)reader[prefix + "Title"],
                Powers = reader.GetEnum<GroupPowers>(prefix + "Powers")
            };
            if (role.ID == UUID.Zero)
            {
                role.Members = (uint)(long)reader["GroupMembers"];
            }
            else
            {
                role.Members = (uint)(long)reader["RoleMembers"];
            }

            return role;
        }

        public static GroupMember ToGroupMember(this NpgsqlDataReader reader) => new GroupMember
        {
            Group = new UGI(reader.GetUUID("GroupID")),
            Principal = reader.GetUGUI("PrincipalID"),
            SelectedRoleID = reader.GetUUID("SelectedRoleID"),
            Contribution = (int)reader["Contribution"],
            IsListInProfile = (bool)reader["ListInProfile"],
            IsAcceptNotices = (bool)reader["AcceptNotices"],
            AccessToken = (string)reader["AccessToken"]
        };

        public static GroupRolemember ToGroupRolemember(this NpgsqlDataReader reader) => new GroupRolemember
        {
            Group = new UGI(reader.GetUUID("GroupID")),
            RoleID = reader.GetUUID("RoleID"),
            Principal = reader.GetUGUI("PrincipalID"),
            Powers = reader.GetEnum<GroupPowers>("Powers")
        };

        public static GroupRolemember ToGroupRolememberEveryone(this NpgsqlDataReader reader, GroupPowers powers) => new GroupRolemember
        {
            Group = new UGI(reader.GetUUID("GroupID")),
            RoleID = UUID.Zero,
            Principal = reader.GetUGUI("PrincipalID"),
            Powers = powers
        };

        public static GroupRolemembership ToGroupRolemembership(this NpgsqlDataReader reader) => new GroupRolemembership
        {
            Group = new UGI(reader.GetUUID("GroupID")),
            RoleID = reader.GetUUID("RoleID"),
            Principal = reader.GetUGUI("PrincipalID"),
            Powers = reader.GetEnum<GroupPowers>("Powers"),
            GroupTitle = (string)reader["Title"]
        };

        public static GroupRolemembership ToGroupRolemembershipEveryone(this NpgsqlDataReader reader, GroupPowers powers) => new GroupRolemembership
        {
            Group = new UGI(reader.GetUUID("GroupID")),
            RoleID = UUID.Zero,
            Principal = reader.GetUGUI("PrincipalID"),
            Powers = powers
        };

        public static GroupInvite ToGroupInvite(this NpgsqlDataReader reader) => new GroupInvite
        {
            ID = reader.GetUUID("InviteID"),
            Group = new UGI(reader.GetUUID("GroupID")),
            RoleID = reader.GetUUID("RoleID"),
            Principal = reader.GetUGUI("PrincipalID"),
            Timestamp = reader.GetDate("Timestamp")
        };

        public static GroupNotice ToGroupNotice(this NpgsqlDataReader reader) => new GroupNotice
        {
            Group = new UGI(reader.GetUUID("GroupID")),
            ID = reader.GetUUID("NoticeID"),
            Timestamp = reader.GetDate("Timestamp"),
            FromName = (string)reader["FromName"],
            Subject = (string)reader["Subject"],
            Message = (string)reader["Message"],
            HasAttachment = (bool)reader["HasAttachment"],
            AttachmentType = reader.GetEnum<AssetType>("AttachmentType"),
            AttachmentName = (string)reader["AttachmentName"],
            AttachmentItemID = reader.GetUUID("AttachmentItemID"),
            AttachmentOwner = reader.GetUGUI("AttachmentOwnerID")
        };
    }
}
