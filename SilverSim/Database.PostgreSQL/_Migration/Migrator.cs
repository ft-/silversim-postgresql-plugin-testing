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
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using PostgreSQLMigrationException = SilverSim.Database.PostgreSQL.PostgreSQLUtilities.PostgreSQLMigrationException;

namespace SilverSim.Database.PostgreSQL._Migration
{
    public static class Migrator
    {
        static void ExecuteStatement(NpgsqlConnection conn, string command, ILog log)
        {
            try
            {
                using (var cmd = new NpgsqlCommand(command, conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
            catch
            {
                log.Debug(command);
                throw;
            }
        }

        static void CreateTable(
            this NpgsqlConnection conn, 
            SqlTable table,
            PrimaryKeyInfo primaryKey,
            Dictionary<string, IColumnInfo> fields,
            Dictionary<string, UniqueKeyInfo> tableKeys,
            uint tableRevision,
            ILog log)
        {
            NpgsqlCommandBuilder b = new NpgsqlCommandBuilder();
            log.InfoFormat("Creating table '{0}' at revision {1}", table.Name, tableRevision);
            var fieldSqls = new List<string>();
            foreach(IColumnInfo field in fields.Values)
            {
                fieldSqls.Add(field.FieldSql());
            }
            if(null != primaryKey)
            {
                fieldSqls.Add(primaryKey.FieldSql());
            }
            foreach(UniqueKeyInfo key in tableKeys.Values)
            {
                fieldSqls.Add(key.FieldSql());
            }

            string escapedTableName = b.QuoteIdentifier(table.Name);
            string cmd = "CREATE TABLE " + escapedTableName + " (";
            cmd += string.Join(",", fieldSqls);
            cmd += ")";
            if(table.IsDynamicRowFormat)
            {
                cmd += " ROW_FORMAT=DYNAMIC";
            }
            cmd += "; COMMENT ON TABLE " + escapedTableName + " IS '" + tableRevision.ToString() + "';";
            ExecuteStatement(conn, cmd, log);
        }

        public static void MigrateTables(this NpgsqlConnection conn, IMigrationElement[] processTable, ILog log)
        {
            NpgsqlCommandBuilder b = new NpgsqlCommandBuilder();
            var tableFields = new Dictionary<string, IColumnInfo>();
            PrimaryKeyInfo primaryKey = null;
            var tableKeys = new Dictionary<string, UniqueKeyInfo>();
            SqlTable table = null;
            uint processingTableRevision = 0;
            uint currentAtRevision = 0;
            NpgsqlTransaction insideTransaction = null;

            if(processTable.Length == 0)
            {
                throw new PostgreSQLMigrationException("Invalid PostgreSQL migration");
            }

            if(null == processTable[0] as SqlTable)
            {
                throw new PostgreSQLMigrationException("First entry must be table name");
            }

            foreach (IMigrationElement migration in processTable)
            {
                Type migrationType = migration.GetType();

                if (typeof(SqlTable) == migrationType)
                {
                    if(insideTransaction != null)
                    {
                        ExecuteStatement(conn, string.Format("COMMENT ON TABLE {0} IS '{1}';", table.Name, processingTableRevision), log);
                        insideTransaction.Commit();
                        insideTransaction = null;
                    }

                    if (null != table && 0 != processingTableRevision)
                    {
                        if(currentAtRevision == 0)
                        {
                            conn.CreateTable(
                                table,
                                primaryKey,
                                tableFields,
                                tableKeys,
                                processingTableRevision,
                                log);
                        }
                        tableKeys.Clear();
                        tableFields.Clear();
                        primaryKey = null;
                    }
                    table = (SqlTable)migration;
                    currentAtRevision = conn.GetTableRevision(table.Name);
                    processingTableRevision = 1;
                }
                else if (typeof(TableRevision) == migrationType)
                {
                    if (insideTransaction != null)
                    {
                        ExecuteStatement(conn, string.Format("COMMENT ON TABLE {0} IS '{1}';", table.Name, processingTableRevision), log);
                        insideTransaction.Commit();
                        insideTransaction = null;
                        if (currentAtRevision != 0)
                        {
                            currentAtRevision = processingTableRevision;
                        }
                    }

                    var rev = (TableRevision)migration;
                    if(rev.Revision != processingTableRevision + 1)
                    {
                        throw new PostgreSQLMigrationException(string.Format("Invalid TableRevision entry. Expected {0}. Got {1}", processingTableRevision + 1, rev.Revision));
                    }

                    processingTableRevision = rev.Revision;

                    if (processingTableRevision - 1 == currentAtRevision && 0 != currentAtRevision)
                    {
                        insideTransaction = conn.BeginTransaction(System.Data.IsolationLevel.Serializable);
                        log.InfoFormat("Migration table '{0}' to revision {1}", table.Name, processingTableRevision);
                    }
                }
                else if (processingTableRevision == 0 || table == null)
                {
                    if (table != null)
                    {
                        throw new PostgreSQLMigrationException("Unexpected processing element for " + table.Name);
                    }
                    else
                    {
                        throw new PostgreSQLMigrationException("Unexpected processing element");
                    }
                }
                else
                {
                    Type[] interfaces = migration.GetType().GetInterfaces();

                    if(interfaces.Contains(typeof(IAddColumn)))
                    {
                        var columnInfo = (IAddColumn)migration;
                        if(tableFields.ContainsKey(columnInfo.Name))
                        {
                            throw new ArgumentException("Column " + columnInfo.Name + " was added twice.");
                        }
                        tableFields.Add(columnInfo.Name, columnInfo);
                        if(insideTransaction != null)
                        {
                            ExecuteStatement(conn, columnInfo.Sql(table.Name), log);
                        }
                    }
                    else if(interfaces.Contains(typeof(IChangeColumn)))
                    {
                        var columnInfo = (IChangeColumn)migration;
                        IColumnInfo oldColumn;
                        if(!tableFields.TryGetValue(columnInfo.Name, out oldColumn))
                        {
                            throw new ArgumentException("Change column for " + columnInfo.Name + " has no preceeding AddColumn");
                        }
                        if(insideTransaction != null)
                        {
                            ExecuteStatement(conn, columnInfo.Sql(table.Name, oldColumn.FieldType), log);
                        }
                        tableFields[columnInfo.Name] = columnInfo;
                    }
                    else if(migrationType == typeof(DropColumn))
                    {
                        var columnInfo = (DropColumn)migration;
                        if (insideTransaction != null)
                        {
                            ExecuteStatement(conn, columnInfo.Sql(table.Name, tableFields[columnInfo.Name].FieldType), log);
                        }
                        tableFields.Remove(columnInfo.Name);
                    }
                    else if(migrationType == typeof(PrimaryKeyInfo))
                    {
                        if(null != primaryKey && insideTransaction != null)
                        {
                            ExecuteStatement(conn, "ALTER TABLE " + b.QuoteIdentifier(table.Name) + " DROP PRIMARY KEY;", log);
                        }
                        primaryKey = (PrimaryKeyInfo)migration;
                        if (insideTransaction != null)
                        {
                            ExecuteStatement(conn, primaryKey.Sql(table.Name), log);
                        }
                    }
                    else if(migrationType == typeof(DropPrimaryKeyinfo))
                    {
                        if (null != primaryKey && insideTransaction != null)
                        {
                            ExecuteStatement(conn, ((DropPrimaryKeyinfo)migration).Sql(table.Name), log);
                        }
                        primaryKey = null;
                    }
                    else if(migrationType == typeof(UniqueKeyInfo))
                    {
                        var namedKey = (UniqueKeyInfo)migration;
                        tableKeys.Add(namedKey.Name, namedKey);
                        if (insideTransaction != null)
                        {
                            ExecuteStatement(conn, namedKey.Sql(table.Name), log);
                        }
                    }
                    else if(migrationType == typeof(DropNamedKeyInfo))
                    {
                        var namedKey = (DropNamedKeyInfo)migration;
                        tableKeys.Remove(namedKey.Name);
                        if (insideTransaction != null)
                        {
                            ExecuteStatement(conn, namedKey.Sql(table.Name), log);
                        }
                    }
                    else
                    {
                        throw new PostgreSQLMigrationException("Invalid type " + migrationType.FullName + " in migration list");
                    }
                }
            }

            if (insideTransaction != null)
            {
                ExecuteStatement(conn, string.Format("COMMENT ON TABLE {0} IS '{1}';", b.QuoteIdentifier(table.Name), processingTableRevision), log);
                insideTransaction.Commit();
                insideTransaction = null;
                if (currentAtRevision != 0)
                {
                    currentAtRevision = processingTableRevision;
                }
            }

            if (null != table && 0 != processingTableRevision && currentAtRevision == 0)
            {
                conn.CreateTable(
                    table,
                    primaryKey,
                    tableFields,
                    tableKeys,
                    processingTableRevision,
                    log);
            }
        }
    }
}