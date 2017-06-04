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
using SilverSim.Scene.Types.SceneEnvironment;
using SilverSim.Types;
using SilverSim.Types.StructuredData.Llsd;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.Serialization;
using System.Text;
using System.Data;
using System.Linq;

namespace SilverSim.Database.PostgreSQL
{
    public static class PostgreSQLUtilities
    {
        private static bool HasOnConflict(this NpgsqlConnection conn)
        {
            Version version = conn.PostgreSqlVersion;
            return version.Major > 9 || (version.Major == 9 && version.Minor >= 5);
        }

        public static string ToNpgsqlQuotedWithoutQuotes(this string unquoted)
        {
            StringBuilder b = new StringBuilder();
            foreach (char c in unquoted)
            {
                if (c == '\'')
                {
                    b.Append("''");
                }
                else
                {
                    b.Append(c);
                }
            }
            return b.ToString();
        }

        public static string ToNpgsqlQuoted(this string unquoted)
        {
            StringBuilder b = new StringBuilder("'");
            foreach(char c in unquoted)
            {
                if(c == '\'')
                {
                    b.Append("''");
                }
                else
                {
                    b.Append(c);
                }
            }
            b.Append("'");
            return b.ToString();
        }

        #region Connection String Creator
        public static string BuildConnectionString(IConfig config, ILog log)
        {
            NpgsqlConnectionStringBuilder sb = new NpgsqlConnectionStringBuilder();
            if (!(config.Contains("Username") && config.Contains("Password") && config.Contains("Database")))
            {
                string configName = config.Name;
                if (!config.Contains("Username"))
                {
                    log.FatalFormat("[POSTGRESQL CONFIG]: Parameter 'Username' missing in [{0}]", configName);
                }
                if (!config.Contains("Password"))
                {
                    log.FatalFormat("[POSTGRESQL CONFIG]: Parameter 'Password' missing in [{0}]", configName);
                }
                if (!config.Contains("Database"))
                {
                    log.FatalFormat("[POSTGRESQL CONFIG]: Parameter 'Database' missing in [{0}]", configName);
                }
                throw new ConfigurationLoader.ConfigurationErrorException();
            }

            if (config.Contains("SslMode"))
            {
                switch(config.GetString("SslMode").ToLower())
                {
                    case "disable":
                        sb.SslMode = SslMode.Disable;
                        break;

                    case "prefer":
                        sb.SslMode = SslMode.Prefer;
                        break;

                    case "require":
                        sb.SslMode = SslMode.Require;
                        break;
                }
            }

            sb.Host = config.GetString("Server", "localhost");

            sb.Username = config.GetString("Username");
            sb.Password = config.GetString("Password");
            sb.Database = config.GetString("Database");

            if(config.Contains("Port"))
            {
                sb.Port = config.GetInt("Port");
            }

            if(config.Contains("MaximumPoolsize"))
            {
                sb.MaxPoolSize = config.GetInt("MaximumPoolsize");
            }

            return sb.ToString();
        }
        #endregion

        #region Exceptions
        [Serializable]
        public class PostgreSQLInsertException : Exception
        {
            public PostgreSQLInsertException()
            {
            }

            public PostgreSQLInsertException(string msg)
                : base(msg)
            {
            }

            protected PostgreSQLInsertException(SerializationInfo info, StreamingContext context)
                : base(info, context)
            {
            }

            public PostgreSQLInsertException(string msg, Exception innerException)
                : base(msg, innerException)
            {
            }
        }

        [Serializable]
        public class PostgreSQLMigrationException : Exception
        {
            public PostgreSQLMigrationException()
            {
            }

            public PostgreSQLMigrationException(string msg)
                : base(msg)
            {
            }

            protected PostgreSQLMigrationException(SerializationInfo info, StreamingContext context)
                : base(info, context)
            {
            }

            public PostgreSQLMigrationException(string msg, Exception innerException)
                : base(msg, innerException)
            {
            }
        }

        [Serializable]
        public class PostgreSQLTransactionException : Exception
        {
            public PostgreSQLTransactionException()
            {
            }

            public PostgreSQLTransactionException(string msg)
                : base(msg)
            {
            }

            protected PostgreSQLTransactionException(SerializationInfo info, StreamingContext context)
                : base(info, context)
            {
            }

            public PostgreSQLTransactionException(string msg, Exception inner)
                : base(msg, inner)
            {
            }
        }
        #endregion

        #region Transaction Helper
        public static void InsideTransaction(this NpgsqlConnection connection, Action del)
        {
            InsideTransaction(connection, IsolationLevel.Serializable, del);
        }

        public static void InsideTransaction(this NpgsqlConnection connection, IsolationLevel level, Action del)
        {
            NpgsqlTransaction transaction = connection.BeginTransaction(level);
            try
            {
                del();
            }
            catch(Exception e)
            {
                transaction.Rollback();
                throw new PostgreSQLTransactionException("Transaction failed", e);
            }
            transaction.Commit();
        }
        #endregion

        #region Push parameters
        public static void AddParameter(this NpgsqlParameterCollection sqlparam, string key, object value)
        {
            var t = value?.GetType();
            if (t == typeof(Vector3))
            {
                var v = (Vector3)value;
                sqlparam.AddWithValue(key + "X", v.X);
                sqlparam.AddWithValue(key + "Y", v.Y);
                sqlparam.AddWithValue(key + "Z", v.Z);
            }
            else if (t == typeof(GridVector))
            {
                var v = (GridVector)value;
                sqlparam.AddWithValue(key + "X", v.X);
                sqlparam.AddWithValue(key + "Y", v.Y);
            }
            else if (t == typeof(Quaternion))
            {
                var v = (Quaternion)value;
                sqlparam.AddWithValue(key + "X", v.X);
                sqlparam.AddWithValue(key + "Y", v.Y);
                sqlparam.AddWithValue(key + "Z", v.Z);
                sqlparam.AddWithValue(key + "W", v.W);
            }
            else if (t == typeof(Color))
            {
                var v = (Color)value;
                sqlparam.AddWithValue(key + "Red", v.R);
                sqlparam.AddWithValue(key + "Green", v.G);
                sqlparam.AddWithValue(key + "Blue", v.B);
            }
            else if (t == typeof(ColorAlpha))
            {
                var v = (ColorAlpha)value;
                sqlparam.AddWithValue(key + "Red", v.R);
                sqlparam.AddWithValue(key + "Green", v.G);
                sqlparam.AddWithValue(key + "Blue", v.B);
                sqlparam.AddWithValue(key + "Alpha", v.A);
            }
            else if (t == typeof(EnvironmentController.WLVector2))
            {
                var vec = (EnvironmentController.WLVector2)value;
                sqlparam.AddWithValue(key + "X", vec.X);
                sqlparam.AddWithValue(key + "Y", vec.Y);
            }
            else if (t == typeof(EnvironmentController.WLVector4))
            {
                var vec = (EnvironmentController.WLVector4)value;
                sqlparam.AddWithValue(key + "Red", vec.X);
                sqlparam.AddWithValue(key + "Green", vec.Y);
                sqlparam.AddWithValue(key + "Blue", vec.Z);
                sqlparam.AddWithValue(key + "Value", vec.W);
            }
            else if (t == typeof(bool))
            {
                sqlparam.AddWithValue(key, (bool)value);
            }
            else if (t == typeof(UUID))
            {
                sqlparam.AddWithValue(key, (Guid)(UUID)value);
            }
            else if (t == typeof(UUI) || t == typeof(UGI) || t == typeof(Uri))
            {
                sqlparam.AddWithValue(key, value.ToString());
            }
            else if (t == typeof(AnArray))
            {
                using (var stream = new MemoryStream())
                {
                    LlsdBinary.Serialize((AnArray)value, stream);
                    sqlparam.AddWithValue(key, stream.ToArray());
                }
            }
            else if (t == typeof(Date))
            {
                sqlparam.AddWithValue(key, ((Date)value).AsLong);
            }
            else if(t == typeof(ulong))
            {
                sqlparam.AddWithValue(key, (long)(ulong)value);
            }
            else if (t == typeof(uint))
            {
                sqlparam.AddWithValue(key, (int)(uint)value);
            }
            else if (t == typeof(ushort))
            {
                sqlparam.AddWithValue(key, (short)(ushort)value);
            }
            else if (t == typeof(byte))
            {
                sqlparam.AddWithValue(key, (short)(byte)value);
            }
            else if (t == typeof(byte))
            {
                sqlparam.AddWithValue(key, (short)(byte)value);
            }
            else if (t.IsEnum)
            {
                Type utype = t.GetEnumUnderlyingType();
                if(utype == typeof(byte) || utype == typeof(sbyte) || utype == typeof(ushort))
                {
                    utype = typeof(short);
                }
                else if(utype == typeof(uint))
                {
                    utype = typeof(int);
                }
                else if (utype == typeof(ulong))
                {
                    utype = typeof(long);
                }

                sqlparam.AddWithValue(key, Convert.ChangeType(value, utype));
            }
            else
            {
                sqlparam.AddWithValue(key, value);
            }
        }

        private static void AddParameters(this NpgsqlParameterCollection sqlparam, Dictionary<string, object> vals)
        {
            foreach (KeyValuePair<string, object> kvp in vals)
            {
                if (kvp.Value != null)
                {
                    AddParameter(sqlparam, "@v_" + kvp.Key, kvp.Value);
                }
            }
        }
        #endregion

        #region REPLACE INTO style helper
        public static void ReplaceInto(this NpgsqlConnection connection, string tablename, Dictionary<string, object> vals, string[] keyfields, bool enableOnConflict)
        {
            bool useOnConflict = connection.HasOnConflict() && enableOnConflict;
            var q = new List<string>();
            foreach (KeyValuePair<string, object> kvp in vals)
            {
                object value = kvp.Value;

                var t = value?.GetType();
                string key = kvp.Key;

                if (t == typeof(Vector3))
                {
                    q.Add(key + "X");
                    q.Add(key + "Y");
                    q.Add(key + "Z");
                }
                else if (t == typeof(GridVector) || t == typeof(EnvironmentController.WLVector2))
                {
                    q.Add(key + "X");
                    q.Add(key + "Y");
                }
                else if (t == typeof(Quaternion))
                {
                    q.Add(key + "X");
                    q.Add(key + "Y");
                    q.Add(key + "Z");
                    q.Add(key + "W");
                }
                else if (t == typeof(Color))
                {
                    q.Add(key + "Red");
                    q.Add(key + "Green");
                    q.Add(key + "Blue");
                }
                else if (t == typeof(EnvironmentController.WLVector4))
                {
                    q.Add(key + "Red");
                    q.Add(key + "Green");
                    q.Add(key + "Blue");
                    q.Add(key + "Value");
                }
                else if (t == typeof(ColorAlpha))
                {
                    q.Add(key + "Red");
                    q.Add(key + "Green");
                    q.Add(key + "Blue");
                    q.Add(key + "Alpha");
                }
                else if (value == null)
                {
                    /* skip */
                }
                else
                {
                    q.Add(key);
                }
            }

            var cb = new NpgsqlCommandBuilder();

            var q1 = new StringBuilder();
            string quotedTableName = cb.QuoteIdentifier(tablename);
            if (useOnConflict)
            {
                var insertIntoFields = new StringBuilder();
                var conflictParams = new StringBuilder();
                var updateParams = new StringBuilder();

                q1.Append("INSERT INTO ");
                q1.Append(quotedTableName);
                q1.Append(" (");
                insertIntoFields.Append(") VALUES (");

                bool first = true;
                foreach (string p in q)
                {
                    if (!first)
                    {
                        q1.Append(",");
                        insertIntoFields.Append(",");
                    }
                    first = false;
                    q1.Append(cb.QuoteIdentifier(p));
                    insertIntoFields.Append("@v_");
                    insertIntoFields.Append(p);
                    if (keyfields.Contains(p))
                    {
                        if (conflictParams.Length != 0)
                        {
                            conflictParams.Append(",");
                        }
                        conflictParams.Append(cb.QuoteIdentifier(p));
                    }
                    else
                    {
                        if (updateParams.Length != 0)
                        {
                            updateParams.Append(",");
                        }
                        updateParams.Append(cb.QuoteIdentifier(p));
                        updateParams.Append("=");
                        updateParams.Append("@v_");
                        updateParams.Append(p);
                    }
                }
                q1.Append(insertIntoFields);
                q1.Append(") ON CONFLICT (");
                q1.Append(conflictParams);
                q1.Append(") DO UPDATE SET ");
                q1.Append(updateParams);
            }
            else
            {
                var insertIntoParams = new StringBuilder();
                var insertIntoFields = new StringBuilder();
                var updateParams = new StringBuilder();
                var whereParams = new StringBuilder();

                foreach (string p in q)
                {
                    string quotedFieldName = cb.QuoteIdentifier(p);
                    if (insertIntoParams.Length != 0)
                    {
                        insertIntoParams.Append(",");
                        insertIntoFields.Append(",");
                    }
                    insertIntoParams.Append("@v_");
                    insertIntoParams.Append(p);
                    insertIntoFields.Append(quotedFieldName);


                    if (keyfields.Contains(p))
                    {
                        if (whereParams.Length != 0)
                        {
                            whereParams.Append(" AND ");
                        }
                        whereParams.Append(quotedFieldName);
                        whereParams.Append(" = ");
                        whereParams.Append("@v_");
                        whereParams.Append(p);
                    }
                    else
                    {
                        if (updateParams.Length != 0)
                        {
                            updateParams.Append(",");
                        }
                        updateParams.Append(quotedFieldName);
                        updateParams.Append("=");
                        updateParams.Append("@v_");
                        updateParams.Append(p);
                    }
                }
                q1.Append("UPDATE ");
                q1.Append(quotedTableName);
                q1.Append(" SET ");
                q1.Append(updateParams);
                q1.Append(" WHERE ");
                q1.Append(whereParams);

                q1.Append("; INSERT INTO ");
                q1.Append(quotedTableName);
                q1.Append(" (");
                q1.Append(insertIntoFields);
                q1.Append(") SELECT ");
                q1.Append(insertIntoParams);
                q1.Append(" WHERE NOT EXISTS (SELECT 1 FROM ");
                q1.Append(quotedTableName);
                q1.Append(" WHERE ");
                q1.Append(whereParams);
                q1.Append(")");
            }

            if (useOnConflict)
            {
                using (var command = new NpgsqlCommand(q1.ToString(), connection))
                {
                    AddParameters(command.Parameters, vals);
                    if (command.ExecuteNonQuery() < 1)
                    {
                        throw new PostgreSQLInsertException();
                    }
                }
            }
            else
            {
                connection.InsideTransaction(() =>
                {
                    using (var command = new NpgsqlCommand(q1.ToString(), connection))
                    {
                        AddParameters(command.Parameters, vals);
                        if (command.ExecuteNonQuery() < 1)
                        {
                            throw new PostgreSQLInsertException();
                        }
                    }
                });
            }
        }
#endregion

#region Common INSERT INTO helper
        public static void InsertInto(this NpgsqlConnection connection, string tablename, Dictionary<string, object> vals)
        {
            var q = new List<string>();
            foreach (KeyValuePair<string, object> kvp in vals)
            {
                object value = kvp.Value;

                var t = value?.GetType();
                string key = kvp.Key;

                if (t == typeof(Vector3))
                {
                    q.Add(key + "X");
                    q.Add(key + "Y");
                    q.Add(key + "Z");
                }
                else if (t == typeof(GridVector) || t == typeof(EnvironmentController.WLVector2))
                {
                    q.Add(key + "X");
                    q.Add(key + "Y");
                }
                else if (t == typeof(Quaternion))
                {
                    q.Add(key + "X");
                    q.Add(key + "Y");
                    q.Add(key + "Z");
                    q.Add(key + "W");
                }
                else if (t == typeof(Color))
                {
                    q.Add(key + "Red");
                    q.Add(key + "Green");
                    q.Add(key + "Blue");
                }
                else if(t == typeof(EnvironmentController.WLVector4))
                {
                    q.Add(key + "Red");
                    q.Add(key + "Green");
                    q.Add(key + "Blue");
                    q.Add(key + "Value");
                }
                else if (t == typeof(ColorAlpha))
                {
                    q.Add(key + "Red");
                    q.Add(key + "Green");
                    q.Add(key + "Blue");
                    q.Add(key + "Alpha");
                }
                else if (value == null)
                {
                    /* skip */
                }
                else
                {
                    q.Add(key);
                }
            }

            var cb = new NpgsqlCommandBuilder();
            var q1 = new StringBuilder();
            var q2 = new StringBuilder();
            q1.Append("INSERT INTO ");
            q1.Append(cb.QuoteIdentifier(tablename));
            q1.Append(" (");
            q2.Append(") VALUES (");
            bool first = true;
            foreach(string p in q)
            {
                if(!first)
                {
                    q1.Append(",");
                    q2.Append(",");
                }
                first = false;
                q1.Append(cb.QuoteIdentifier(p));
                q2.Append("@v_");
                q2.Append(p);
            }
            q1.Append(q2);
            q1.Append(")");
            using (var command = new NpgsqlCommand(q1.ToString(), connection))
            {
                AddParameters(command.Parameters, vals);
                if (command.ExecuteNonQuery() < 1)
                {
                    throw new PostgreSQLInsertException();
                }
            }
        }
#endregion

#region Generate values
        public static string GenerateFieldNames(Dictionary<string, object> vals)
        {
            var q = new List<string>();
            foreach (KeyValuePair<string, object> kvp in vals)
            {
                object value = kvp.Value;

                var t = value?.GetType();
                string key = kvp.Key;

                if (t == typeof(Vector3))
                {
                    q.Add(key + "X");
                    q.Add(key + "Y");
                    q.Add(key + "Z");
                }
                else if (t == typeof(GridVector) || t == typeof(EnvironmentController.WLVector2))
                {
                    q.Add(key + "X");
                    q.Add(key + "Y");
                }
                else if (t == typeof(Quaternion))
                {
                    q.Add(key + "X");
                    q.Add(key + "Y");
                    q.Add(key + "Z");
                    q.Add(key + "W");
                }
                else if (t == typeof(Color))
                {
                    q.Add(key + "Red");
                    q.Add(key + "Green");
                    q.Add(key + "Blue");
                }
                else if (t == typeof(EnvironmentController.WLVector4))
                {
                    q.Add(key + "Red");
                    q.Add(key + "Green");
                    q.Add(key + "Blue");
                    q.Add(key + "Value");
                }
                else if (t == typeof(ColorAlpha))
                {
                    q.Add(key + "Red");
                    q.Add(key + "Green");
                    q.Add(key + "Blue");
                    q.Add(key + "Alpha");
                }
                else
                {
                    q.Add(key);
                }
            }

            var q1 = new StringBuilder();
            foreach (string p in q)
            {
                if (q1.Length != 0)
                {
                    q1.Append(",");
                }
                q1.Append("`");
                q1.Append(p);
                q1.Append("`");
            }
            return q1.ToString();
        }

        public static string GenerateValues(Dictionary<string, object> vals)
        {
            var resvals = new List<string>();

            foreach (object value in vals.Values)
            {
                var t = value?.GetType();
                if (t == typeof(Vector3))
                {
                    var v = (Vector3)value;
                    resvals.Add(v.X.ToString(CultureInfo.InvariantCulture));
                    resvals.Add(v.Y.ToString(CultureInfo.InvariantCulture));
                    resvals.Add(v.Z.ToString(CultureInfo.InvariantCulture));
                }
                else if (t == typeof(GridVector))
                {
                    var v = (GridVector)value;
                    resvals.Add(v.X.ToString(CultureInfo.InvariantCulture));
                    resvals.Add(v.Y.ToString(CultureInfo.InvariantCulture));
                }
                else if (t == typeof(Quaternion))
                {
                    var v = (Quaternion)value;
                    resvals.Add(v.X.ToString(CultureInfo.InvariantCulture));
                    resvals.Add(v.Y.ToString(CultureInfo.InvariantCulture));
                    resvals.Add(v.Z.ToString(CultureInfo.InvariantCulture));
                    resvals.Add(v.W.ToString(CultureInfo.InvariantCulture));
                }
                else if (t == typeof(Color))
                {
                    var v = (Color)value;
                    resvals.Add(v.R.ToString(CultureInfo.InvariantCulture));
                    resvals.Add(v.G.ToString(CultureInfo.InvariantCulture));
                    resvals.Add(v.B.ToString(CultureInfo.InvariantCulture));
                }
                else if (t == typeof(ColorAlpha))
                {
                    var v = (ColorAlpha)value;
                    resvals.Add(v.R.ToString(CultureInfo.InvariantCulture));
                    resvals.Add(v.G.ToString(CultureInfo.InvariantCulture));
                    resvals.Add(v.B.ToString(CultureInfo.InvariantCulture));
                    resvals.Add(v.A.ToString(CultureInfo.InvariantCulture));
                }
                else if (t == typeof(EnvironmentController.WLVector2))
                {
                    var v = (EnvironmentController.WLVector2)value;
                    resvals.Add(v.X.ToString(CultureInfo.InvariantCulture));
                    resvals.Add(v.Y.ToString(CultureInfo.InvariantCulture));
                }
                else if (t == typeof(EnvironmentController.WLVector4))
                {
                    var v = (EnvironmentController.WLVector4)value;
                    resvals.Add(v.X.ToString(CultureInfo.InvariantCulture));
                    resvals.Add(v.Y.ToString(CultureInfo.InvariantCulture));
                    resvals.Add(v.Z.ToString(CultureInfo.InvariantCulture));
                    resvals.Add(v.W.ToString(CultureInfo.InvariantCulture));
                }
                else if (t == typeof(bool))
                {
                    resvals.Add((bool)value ? "1" : "0");
                }
                else if (t == typeof(UUID) || t == typeof(UUI) || t == typeof(UGI) || t == typeof(Uri) || t == typeof(string))
                {
                    resvals.Add(value.ToString().ToNpgsqlQuoted());
                }
                else if (t == typeof(AnArray))
                {
                    using (var stream = new MemoryStream())
                    {
                        LlsdBinary.Serialize((AnArray)value, stream);
                        byte[] b = stream.ToArray();
                        resvals.Add(b.Length == 0 ? "''" : "0x" + b.ToHexString());
                    }
                }
                else if(t == typeof(byte[]))
                {
                    var b = (byte[])value;
                    resvals.Add(b.Length == 0 ? "''" : "0x" + b.ToHexString());
                }
                else if (t == typeof(Date))
                {
                    resvals.Add(((Date)value).AsULong.ToString());
                }
                else if (t == typeof(float))
                {
                    resvals.Add(((float)value).ToString(CultureInfo.InvariantCulture));
                }
                else if (t == typeof(double))
                {
                    resvals.Add(((double)value).ToString(CultureInfo.InvariantCulture));
                }
                else if (value == null)
                {
                    resvals.Add("NULL");
                }
                else if (t.IsEnum)
                {
                    resvals.Add(Convert.ChangeType(value, t.GetEnumUnderlyingType()).ToString());
                }
                else
                {
                    resvals.Add(value.ToString());
                }
            }
            return string.Join(",", resvals);
        }
#endregion

#region UPDATE SET helper
        private static List<string> UpdateSetFromVals(Dictionary<string, object> vals)
        {
            var updates = new List<string>();

            foreach (KeyValuePair<string, object> kvp in vals)
            {
                object value = kvp.Value;
                var t = value?.GetType();
                string key = kvp.Key;

                if (t == typeof(Vector3))
                {
                    updates.Add("`" + key + "X` = @v_" + key + "X");
                    updates.Add("`" + key + "Y` = @v_" + key + "Y");
                    updates.Add("`" + key + "Z` = @v_" + key + "Z");
                }
                else if (t == typeof(GridVector) || t == typeof(EnvironmentController.WLVector2))
                {
                    updates.Add("`" + key + "X` = @v_" + key + "X");
                    updates.Add("`" + key + "Y` = @v_" + key + "Y");
                }
                else if (t == typeof(Quaternion))
                {
                    updates.Add("`" + key + "X` = @v_" + key + "X");
                    updates.Add("`" + key + "Y` = @v_" + key + "Y");
                    updates.Add("`" + key + "Z` = @v_" + key + "Z");
                    updates.Add("`" + key + "W` = @v_" + key + "W");
                }
                else if (t == typeof(Color))
                {
                    updates.Add("`" + key + "Red` = @v_" + key + "Red");
                    updates.Add("`" + key + "Green` = @v_" + key + "Green");
                    updates.Add("`" + key + "Blue` = @v_" + key + "Blue");
                }
                else if (t == typeof(EnvironmentController.WLVector4))
                {
                    updates.Add("`" + key + "Red` = @v_" + key + "Red");
                    updates.Add("`" + key + "Green` = @v_" + key + "Green");
                    updates.Add("`" + key + "Blue` = @v_" + key + "Blue");
                    updates.Add("`" + key + "Value` = @v_" + key + "Value");
                }
                else if (t == typeof(ColorAlpha))
                {
                    updates.Add("`" + key + "Red` = @v_" + key + "Red");
                    updates.Add("`" + key + "Green` = @v_" + key + "Green");
                    updates.Add("`" + key + "Blue` = @v_" + key + "Blue");
                    updates.Add("`" + key + "Alpha` = @v_" + key + "Alpha");
                }
                else if (value == null)
                {
                    /* skip */
                }
                else
                {
                    updates.Add("`" + key + "` = @v_" + key);
                }
            }
            return updates;
        }

        public static void UpdateSet(this NpgsqlConnection connection, string tablename, Dictionary<string, object> vals, string where)
        {
            string q1 = "UPDATE " + tablename + " SET ";

            q1 += string.Join(",", UpdateSetFromVals(vals));

            using (var command = new NpgsqlCommand(q1 + " WHERE " + where, connection))
            {
                AddParameters(command.Parameters, vals);
                if (command.ExecuteNonQuery() < 1)
                {
                    throw new PostgreSQLInsertException();
                }
            }
        }

        public static void UpdateSet(this NpgsqlConnection connection, string tablename, Dictionary<string, object> vals, Dictionary<string, object> where)
        {
            string q1 = "UPDATE " + tablename + " SET ";

            q1 += string.Join(",", UpdateSetFromVals(vals));

            var wherestr = new StringBuilder();
            foreach(KeyValuePair<string, object> w in where)
            {
                if(wherestr.Length != 0)
                {
                    wherestr.Append(" AND ");
                }
                wherestr.AppendFormat("{0} = @w_{0}", w.Key);
            }

            using (var command = new NpgsqlCommand(q1 + " WHERE " + wherestr, connection))
            {
                AddParameters(command.Parameters, vals);
                foreach(KeyValuePair<string, object> w in where)
                {
                    command.Parameters.AddWithValue("@w_" + w.Key, w.Value);
                }
                if (command.ExecuteNonQuery() < 1)
                {
                    throw new PostgreSQLInsertException();
                }
            }
        }
#endregion

#region Data parsers
        public static EnvironmentController.WLVector4 GetWLVector4(this NpgsqlDataReader dbReader, string prefix) =>
            new EnvironmentController.WLVector4(
                (double)dbReader[prefix + "Red"],
                (double)dbReader[prefix + "Green"],
                (double)dbReader[prefix + "Blue"],
                (double)dbReader[prefix + "Value"]);

        public static T GetEnum<T>(this NpgsqlDataReader dbreader, string prefix)
        {
            var enumType = typeof(T).GetEnumUnderlyingType();
            object v = dbreader[prefix];
            return (T)Convert.ChangeType(v, enumType);
        }

        public static UUID GetUUID(this NpgsqlDataReader dbReader, string prefix)
        {
            object v = dbReader[prefix];
            var t = v?.GetType();
            if(t == typeof(Guid))
            {
                return new UUID((Guid)v);
            }

            if(t == typeof(string))
            {
                return new UUID((string)v);
            }

            throw new InvalidCastException("GetUUID could not convert value for " + prefix);
        }

        public static UUI GetUUI(this NpgsqlDataReader dbReader, string prefix)
        {
            object v = dbReader[prefix];
            var t = v?.GetType();
            if (t == typeof(Guid))
            {
                return new UUI((Guid)v);
            }

            if (t == typeof(string))
            {
                return new UUI((string)v);
            }

            throw new InvalidCastException("GetUUI could not convert value for " + prefix);
        }

        public static UGI GetUGI(this NpgsqlDataReader dbReader, string prefix)
        {
            object v = dbReader[prefix];
            var t = v?.GetType();
            if (t == typeof(Guid))
            {
                return new UGI((Guid)v);
            }

            if (t == typeof(string))
            {
                return new UGI((string)v);
            }

            throw new InvalidCastException("GetUGI could not convert value for " + prefix);
        }

        public static Date GetDate(this NpgsqlDataReader dbReader, string prefix)
        {
            ulong v;
            if (!ulong.TryParse(dbReader[prefix].ToString(), out v))
            {
                throw new InvalidCastException("GetDate could not convert value for "+ prefix);
            }
            return Date.UnixTimeToDateTime(v);
        }

        public static EnvironmentController.WLVector2 GetWLVector2(this NpgsqlDataReader dbReader, string prefix) =>
            new EnvironmentController.WLVector2(
                (double)dbReader[prefix + "X"],
                (double)dbReader[prefix + "Y"]);

        public static Vector3 GetVector3(this NpgsqlDataReader dbReader, string prefix) =>
            new Vector3(
                (double)dbReader[prefix + "X"],
                (double)dbReader[prefix + "Y"],
                (double)dbReader[prefix + "Z"]);

        public static Quaternion GetQuaternion(this NpgsqlDataReader dbReader, string prefix) =>
            new Quaternion(
                (double)dbReader[prefix + "X"],
                (double)dbReader[prefix + "Y"],
                (double)dbReader[prefix + "Z"],
                (double)dbReader[prefix + "W"]);

        public static Color GetColor(this NpgsqlDataReader dbReader, string prefix) =>
            new Color(
                (double)dbReader[prefix + "Red"],
                (double)dbReader[prefix + "Green"],
                (double)dbReader[prefix + "Blue"]);

        public static ColorAlpha GetColorAlpha(this NpgsqlDataReader dbReader, string prefix) =>
            new ColorAlpha(
                (double)dbReader[prefix + "Red"],
                (double)dbReader[prefix + "Green"],
                (double)dbReader[prefix + "Blue"],
                (double)dbReader[prefix + "Alpha"]);

        public static byte[] GetBytes(this NpgsqlDataReader dbReader, string prefix)
        {
            object o = dbReader[prefix];
            var t = o?.GetType();
            if(t == typeof(DBNull))
            {
                return new byte[0];
            }
            return (byte[])o;
        }

        public static Uri GetUri(this NpgsqlDataReader dbReader, string prefix)
        {
            object o = dbReader[prefix];
            var t = o?.GetType();
            if(t == typeof(DBNull))
            {
                return null;
            }
            var s = (string)o;
            if(s.Length == 0)
            {
                return null;
            }
            return new Uri(s);
        }

        public static GridVector GetGridVector(this NpgsqlDataReader dbReader, string prefix) =>
            new GridVector((uint)dbReader[prefix + "X"], (uint)dbReader[prefix + "Y"]);
#endregion

#region Migrations helper
        public static uint GetTableRevision(this NpgsqlConnection connection, string name)
        {
            using(var cmd = new NpgsqlCommand("SELECT description FROM pg_description " +
                        "JOIN pg_class ON pg_description.objoid = pg_class.oid " +
                        "JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid " +
                        "WHERE relname = @name", connection))
            {
                cmd.Parameters.AddWithValue("@name", name);
                using (NpgsqlDataReader dbReader = cmd.ExecuteReader())
                {
                    if (dbReader.Read())
                    {
                        uint u;
                        if(!uint.TryParse((string)dbReader["description"], out u))
                        {
                            throw new InvalidDataException("description is not a parseable number");
                        }
                        return u;
                    }
                }
            }
            return 0;
        }
#endregion
    }
}
