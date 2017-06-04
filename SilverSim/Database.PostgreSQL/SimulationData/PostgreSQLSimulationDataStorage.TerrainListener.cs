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
using SilverSim.ServiceInterfaces.Statistics;
using SilverSim.Threading;
using SilverSim.Types;
using SilverSim.Viewer.Messages.LayerData;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace SilverSim.Database.PostgreSQL.SimulationData
{
    public sealed partial class PostgreSQLSimulationDataStorage
    {
        readonly RwLockedList<PostgreSQLTerrainListener> m_TerrainListenerThreads = new RwLockedList<PostgreSQLTerrainListener>();
        public class PostgreSQLTerrainListener : TerrainListener
        {
            private readonly RwLockedList<PostgreSQLTerrainListener> m_TerrainListenerThreads;
            private readonly string m_ConnectionString;
            private readonly bool m_EnableOnConflict;

            public PostgreSQLTerrainListener(string connectionString, UUID regionID, RwLockedList<PostgreSQLTerrainListener> terrainListenerThreads, bool enableOnConflict)
            {
                m_ConnectionString = connectionString;
                RegionID = regionID;
                m_TerrainListenerThreads = terrainListenerThreads;
                m_EnableOnConflict = enableOnConflict;
            }

            public UUID RegionID { get; }

            public QueueStat GetStats()
            {
                int count = m_StorageTerrainRequestQueue.Count;
                return new QueueStat(count != 0 ? "PROCESSING" : "IDLE", count, (uint)m_ProcessedPatches);
            }

            private int m_ProcessedPatches;

            protected override void StorageTerrainThread()
            {
                try
                {
                    m_TerrainListenerThreads.Add(this);
                    Thread.CurrentThread.Name = "Storage Terrain Thread: " + RegionID.ToString();

                    var knownSerialNumbers = new C5.TreeDictionary<uint, uint>();
                    Dictionary<string, object> updateRequestData = new Dictionary<string, object>();
                    int updateRequestCount = 0;

                    while (!m_StopStorageThread || m_StorageTerrainRequestQueue.Count != 0)
                    {
                        LayerPatch req;
                        try
                        {
                            req = m_StorageTerrainRequestQueue.Dequeue(1000);
                        }
                        catch
                        {
                            continue;
                        }

                        uint serialNumber = req.Serial;

                        if (!knownSerialNumbers.Contains(req.ExtendedPatchID) || knownSerialNumbers[req.ExtendedPatchID] != req.Serial)
                        {
                            var data = new Dictionary<string, object>
                            {
                                ["RegionID" + updateRequestCount] = RegionID,
                                ["PatchID" + updateRequestCount] = req.ExtendedPatchID,
                                ["TerrainData" + updateRequestCount] = req.Serialization
                            };
                            ++updateRequestCount;
                            knownSerialNumbers[req.ExtendedPatchID] = serialNumber;
                        }

                        if ((m_StorageTerrainRequestQueue.Count == 0 && updateRequestCount > 0) || updateRequestCount >= 256)
                        {
                            StringBuilder updateCmd = new StringBuilder();
                            try
                            {
                                using (NpgsqlConnection conn = new NpgsqlConnection(m_ConnectionString))
                                {
                                    conn.Open();
                                    if (conn.HasOnConflict() && m_EnableOnConflict)
                                    {
                                        for (int i = 0; i < updateRequestCount; ++i)
                                        {
                                            updateCmd.AppendFormat("INSERT INTO terrains (\"RegionID\", \"PatchID\", \"TerrainData\") VALUES (@regionid, @patchid{0}, @terraindata{0}) ON CONFLICT(\"RegionID\", \"PatchID\") DO UPDATE SET \"TerrainData\"= @terraindata{0};", i);
                                        }
                                    }
                                    else
                                    {
                                        for (int i = 0; i < updateRequestCount; ++i)
                                        {
                                            updateCmd.AppendFormat("UPDATE terrains SET \"TerrainData\"=@terraindata{0} WHERE \"RegionID\" = @regionid AND \"PatchID\" = @patchid{0};", i);
                                            updateCmd.AppendFormat("INSERT INTO terrains (\"RegionID\", \"PatchID\", \"TerrainData\") SELECT @regionid, @patchid{0}, @terraindata{0} WHERE NOT EXISTS " +
                                                    "(SELECT 1 FROM terrains WHERE \"RegionID\" = @regionid AND \"PatchID\" = @patchid{0});", i);
                                        }
                                    }
                                    using (NpgsqlCommand cmd = new NpgsqlCommand(updateCmd.ToString(), conn))
                                    {
                                        cmd.Parameters.AddParameter("@regionid", RegionID);
                                        foreach (KeyValuePair<string, object> kvp in updateRequestData)
                                        {
                                            cmd.Parameters.AddParameter(kvp.Key, kvp.Value);
                                        }
                                        cmd.ExecuteNonQuery();
                                    }
                                }
                                updateRequestData.Clear();
                                updateRequestCount = 0;
                                Interlocked.Increment(ref m_ProcessedPatches);
                            }
                            catch (Exception e)
                            {
                                m_Log.Error("Terrain store failed", e);
                            }
                        }
                    }
                }
                finally
                {
                    m_TerrainListenerThreads.Remove(this);
                }
            }
        }

        public override TerrainListener GetTerrainListener(UUID regionID) =>
            new PostgreSQLTerrainListener(m_ConnectionString, regionID, m_TerrainListenerThreads, m_EnableOnConflict);
    }
}
