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
using SilverSim.Types;
using SilverSim.Types.Parcel;
using System.Collections.Generic;

namespace SilverSim.Database.PostgreSQL.SimulationData
{
    public sealed partial class PostgreSQLSimulationDataStorage : ISimulationDataParcelStorageInterface
    {
        private readonly PostgreSQLSimulationDataParcelAccessListStorage m_WhiteListStorage;
        private readonly PostgreSQLSimulationDataParcelAccessListStorage m_BlackListStorage;
        private readonly PostgreSQLSimulationDataParcelAccessListStorage m_LandpassListStorage;

        ParcelInfo ISimulationDataParcelStorageInterface.this[UUID regionID, UUID parcelID]
        {
            get
            {
                using (var connection = new NpgsqlConnection(m_ConnectionString))
                {
                    connection.Open();
                    using (var cmd = new NpgsqlCommand("SELECT * FROM parcels WHERE \"RegionID\" = @regionid AND \"ParcelID\" = @parcelid", connection))
                    {
                        cmd.Parameters.AddParameter("@regionid", regionID);
                        cmd.Parameters.AddParameter("@parcelid", parcelID);
                        using (NpgsqlDataReader dbReader = cmd.ExecuteReader())
                        {
                            if (!dbReader.Read())
                            {
                                throw new KeyNotFoundException();
                            }

                            var pi = new ParcelInfo((int)dbReader["BitmapWidth"], (int)dbReader["BitmapHeight"])
                            {
                                Area = (int)dbReader["Area"],
                                AuctionID = (uint)(int)dbReader["AuctionID"],
                                AuthBuyer = dbReader.GetUUI("AuthBuyer"),
                                Category = dbReader.GetEnum<ParcelCategory>("Category"),
                                ClaimDate = dbReader.GetDate("ClaimDate"),
                                ClaimPrice = (int)dbReader["ClaimPrice"],
                                ID = dbReader.GetUUID("ParcelID"),
                                Group = dbReader.GetUGI("Group"),
                                GroupOwned = (bool)dbReader["IsGroupOwned"],
                                Description = (string)dbReader["Description"],
                                Flags = dbReader.GetEnum<ParcelFlags>("Flags"),
                                LandingType = dbReader.GetEnum<TeleportLandingType>("LandingType"),
                                LandingPosition = dbReader.GetVector3("LandingPosition"),
                                LandingLookAt = dbReader.GetVector3("LandingLookAt"),
                                Name = (string)dbReader["Name"],
                                LocalID = (int)dbReader["LocalID"],
                                MediaID = dbReader.GetUUID("MediaID"),
                                Owner = dbReader.GetUUI("Owner"),
                                SnapshotID = dbReader.GetUUID("SnapshotID"),
                                SalePrice = (int)dbReader["SalePrice"],
                                OtherCleanTime = (int)dbReader["OtherCleanTime"],
                                MediaAutoScale = (bool)dbReader["MediaAutoScale"],
                                MediaType = (string)dbReader["MediaType"],
                                MediaWidth = (int)dbReader["MediaWidth"],
                                MediaHeight = (int)dbReader["MediaHeight"],
                                MediaLoop = (bool)dbReader["MediaLoop"],
                                ObscureMedia = (bool)dbReader["ObscureMedia"],
                                ObscureMusic = (bool)dbReader["ObscureMusic"],
                                MediaDescription = (string)dbReader["MediaDescription"],
                                RentPrice = (int)dbReader["RentPrice"],
                                AABBMin = dbReader.GetVector3("AABBMin"),
                                AABBMax = dbReader.GetVector3("AABBMax"),
                                ParcelPrimBonus = (double)dbReader["ParcelPrimBonus"],
                                PassPrice = (int)dbReader["PassPrice"],
                                PassHours = (double)dbReader["PassHours"],
                                ActualArea = (int)dbReader["ActualArea"],
                                BillableArea = (int)dbReader["BillAbleArea"],
                                Status = dbReader.GetEnum<ParcelStatus>("Status"),
                                SeeAvatars = (bool)dbReader["SeeAvatars"],
                                AnyAvatarSounds = (bool)dbReader["AnyAvatarSounds"],
                                GroupAvatarSounds = (bool)dbReader["GroupAvatarSounds"],
                                IsPrivate = (bool)dbReader["IsPrivate"]
                            };
                            pi.LandBitmap.DataNoAABBUpdate = dbReader.GetBytes("Bitmap");

                            var uri = (string)dbReader["MusicURI"];
                            if (!string.IsNullOrEmpty(uri))
                            {
                                pi.MusicURI = new URI(uri);
                            }
                            uri = (string)dbReader["MediaURI"];
                            if (!string.IsNullOrEmpty(uri))
                            {
                                pi.MediaURI = new URI(uri);
                            }

                            return pi;
                        }
                    }
                }
            }
        }

        bool ISimulationDataParcelStorageInterface.Remove(UUID regionID, UUID parcelID)
        {
            using (var connection = new NpgsqlConnection(m_ConnectionString))
            {
                connection.Open();
                using (var cmd = new NpgsqlCommand("DELETE FROM parcels WHERE \"RegionID\" = @regionid AND \"ParcelID\" = @parcelid", connection))
                {
                    cmd.Parameters.AddParameter("@regionid", regionID);
                    cmd.Parameters.AddParameter("@parcelid", parcelID);
                    return cmd.ExecuteNonQuery() > 0;
                }
            }
        }

        List<UUID> ISimulationDataParcelStorageInterface.ParcelsInRegion(UUID key)
        {
            var parcels = new List<UUID>();
            using (var connection = new NpgsqlConnection(m_ConnectionString))
            {
                connection.Open();
                using (var cmd = new NpgsqlCommand("SELECT \"ParcelID\" FROM parcels WHERE \"RegionID\" = @regionid", connection))
                {
                    cmd.Parameters.AddParameter("@regionid", key);
                    using (NpgsqlDataReader dbReader = cmd.ExecuteReader())
                    {
                        while (dbReader.Read())
                        {
                            parcels.Add(dbReader.GetUUID("ParcelID"));
                        }
                    }
                }
            }
            return parcels;
        }

        void ISimulationDataParcelStorageInterface.Store(UUID regionID, ParcelInfo parcel)
        {
            var p = new Dictionary<string, object>
            {
                ["RegionID"] = regionID,
                ["ParcelID"] = parcel.ID,
                ["LocalID"] = parcel.LocalID,
                ["Bitmap"] = parcel.LandBitmap.Data,
                ["BitmapWidth"] = parcel.LandBitmap.BitmapWidth,
                ["BitmapHeight"] = parcel.LandBitmap.BitmapHeight,
                ["Name"] = parcel.Name,
                ["Description"] = parcel.Description,
                ["Owner"] = parcel.Owner,
                ["IsGroupOwned"] = parcel.GroupOwned,
                ["Area"] = parcel.Area,
                ["AuctionID"] = parcel.AuctionID,
                ["AuthBuyer"] = parcel.AuthBuyer,
                ["Category"] = parcel.Category,
                ["ClaimDate"] = parcel.ClaimDate.AsULong,
                ["ClaimPrice"] = parcel.ClaimPrice,
                ["Group"] = parcel.Group,
                ["Flags"] = parcel.Flags,
                ["LandingType"] = parcel.LandingType,
                ["LandingPosition"] = parcel.LandingPosition,
                ["LandingLookAt"] = parcel.LandingLookAt,
                ["Status"] = parcel.Status,
                ["MusicURI"] = parcel.MusicURI,
                ["MediaURI"] = parcel.MediaURI,
                ["MediaType"] = parcel.MediaType,
                ["MediaWidth"] = parcel.MediaWidth,
                ["MediaHeight"] = parcel.MediaHeight,
                ["MediaID"] = parcel.MediaID,
                ["SnapshotID"] = parcel.SnapshotID,
                ["SalePrice"] = parcel.SalePrice,
                ["OtherCleanTime"] = parcel.OtherCleanTime,
                ["MediaAutoScale"] = parcel.MediaAutoScale,
                ["MediaDescription"] = parcel.MediaDescription,
                ["MediaLoop"] = parcel.MediaLoop,
                ["ObscureMedia"] = parcel.ObscureMedia,
                ["ObscureMusic"] = parcel.ObscureMusic,
                ["RentPrice"] = parcel.RentPrice,
                ["AABBMin"] = parcel.AABBMin,
                ["AABBMax"] = parcel.AABBMax,
                ["ParcelPrimBonus"] = parcel.ParcelPrimBonus,
                ["PassPrice"] = parcel.PassPrice,
                ["PassHours"] = parcel.PassHours,
                ["ActualArea"] = parcel.ActualArea,
                ["BillableArea"] = parcel.BillableArea,
                ["SeeAvatars"] = parcel.SeeAvatars,
                ["AnyAvatarSounds"] = parcel.AnyAvatarSounds,
                ["GroupAvatarSounds"] = parcel.GroupAvatarSounds,
                ["IsPrivate"] = parcel.IsPrivate
            };
            using (var connection = new NpgsqlConnection(m_ConnectionString))
            {
                connection.Open();
                connection.ReplaceInto("parcels", p, new string[] { "RegionID", "ParcelID" }, m_EnableOnConflict);
            }
        }

        ISimulationDataParcelAccessListStorageInterface ISimulationDataParcelStorageInterface.WhiteList => m_WhiteListStorage;

        ISimulationDataParcelAccessListStorageInterface ISimulationDataParcelStorageInterface.BlackList => m_BlackListStorage;

        ISimulationDataParcelAccessListStorageInterface ISimulationDataParcelStorageInterface.LandpassList => m_LandpassListStorage;

        ISimulationDataParcelExperienceListStorageInterface ISimulationDataParcelStorageInterface.Experiences => this;
    }
}
