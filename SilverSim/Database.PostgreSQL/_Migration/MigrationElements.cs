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
using SilverSim.Scene.Types.SceneEnvironment;
using SilverSim.Types;
using System;
using System.Collections.Generic;
using System.Globalization;

namespace SilverSim.Database.PostgreSQL._Migration
{
    public interface IMigrationElement
    {
        string Sql(string tableName);
    }

    public class SqlTable : IMigrationElement
    {
        public string Name { get; }
        
        public SqlTable(string name)
        {
            Name = name;
        }

        public string Sql(string tableName)
        {
            throw new NotSupportedException();
        }
    }

    public class PrimaryKeyInfo : IMigrationElement
    {
        public string[] FieldNames { get; }

        public PrimaryKeyInfo(params string[] fieldNames)
        {
            FieldNames = fieldNames;
        }

        public PrimaryKeyInfo(PrimaryKeyInfo src)
        {
            FieldNames = new string[src.FieldNames.Length];
            for (int i = 0; i < src.FieldNames.Length; ++i)
            {
                FieldNames[i] = src.FieldNames[i];
            }
        }

        public string FieldSql()
        {
            NpgsqlCommandBuilder b = new NpgsqlCommandBuilder();
            var fieldNames = new List<string>();
            foreach (string fName in FieldNames)
            {
                fieldNames.Add(b.QuoteIdentifier(fName));
            }
            return "PRIMARY KEY(" + string.Join(",", fieldNames) + ")";
        }

        public string Sql(string tableName) => "ALTER TABLE " + tableName + " ADD " + FieldSql() + ";";
    }

    public class DropPrimaryKeyinfo : IMigrationElement
    {
        public string Sql(string tableName) => "ALTER TABLE " + tableName + " DROP PRIMARY KEY;";
    }

    public class NamedKeyInfo : IMigrationElement
    {
        public bool IsUnique { get; set; }
        public string Name { get; }
        public string[] FieldNames { get; }

        public NamedKeyInfo(string name, params string[] fieldNames)
        {
            Name = name;
            FieldNames = fieldNames;
        }

        public NamedKeyInfo(NamedKeyInfo src)
        {
            IsUnique = src.IsUnique;
            Name = src.Name;
            FieldNames = new string[src.FieldNames.Length];
            for (int i = 0; i < src.FieldNames.Length; ++i)
            {
                FieldNames[i] = src.FieldNames[i];
            }
        }

        private string FieldSql()
        {
            NpgsqlCommandBuilder b = new NpgsqlCommandBuilder();
            var fieldNames = new List<string>();
            foreach (string fName in FieldNames)
            {
                fieldNames.Add(b.QuoteIdentifier(fName));
            }
            return "(" + string.Join(",", fieldNames) + ")";
        }

        public string Sql(string tableName)
        {
            NpgsqlCommandBuilder b = new NpgsqlCommandBuilder();
            return "CREATE " + (IsUnique ? " UNIQUE " : "") + " INDEX " + b.QuoteIdentifier(tableName + "_" + Name) + " ON " + b.QuoteIdentifier(tableName) + " " + FieldSql() + ";";
        }
    }

    public class DropNamedKeyInfo : IMigrationElement
    {
        public string Name { get; }

        public DropNamedKeyInfo(string name)
        {
            Name = name;
        }

        public string Sql(string tableName) => "DROP INDEX " + new NpgsqlCommandBuilder().QuoteIdentifier(tableName + "_" + Name) + ";";
    }

    #region Table fields
    public interface IColumnInfo
    {
        string Name { get; }
        Type FieldType { get; }
        uint Cardinality { get; }
        bool IsNullAllowed { get; }
        bool IsLong { get; }
        bool IsFixed { get; }
        object Default { get; }
        string FieldSql();
    }

    public interface IAddColumn : IColumnInfo
    {
        string Sql(string tableName);
    }

    static class ColumnGenerator
    {
        public static Dictionary<string, string> DropDefault(this IColumnInfo colInfo)
        {
            var result = new Dictionary<string, string>();
            var t = new NpgsqlCommandBuilder();
            Type f = colInfo.FieldType;
            string cmdgen = "ALTER {0} DROP DEFAULT";

            if (f == typeof(Vector3))
            {
                result.Add(colInfo.Name + "X", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "X")));
                result.Add(colInfo.Name + "Y", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Y")));
                result.Add(colInfo.Name + "Z", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Z")));
                return result;
            }
            else if (f == typeof(GridVector))
            {
                result.Add(colInfo.Name + "X", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "X")));
                result.Add(colInfo.Name + "Y", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Y")));
                return result;
            }
            else if (f == typeof(Vector4))
            {
                result.Add(colInfo.Name + "X", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "X")));
                result.Add(colInfo.Name + "Y", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Y")));
                result.Add(colInfo.Name + "Z", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Z")));
                result.Add(colInfo.Name + "W", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "W")));
                return result;
            }
            else if (f == typeof(Quaternion))
            {
                result.Add(colInfo.Name + "X", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "X")));
                result.Add(colInfo.Name + "Y", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Y")));
                result.Add(colInfo.Name + "Z", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Z")));
                result.Add(colInfo.Name + "W", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "W")));
                return result;
            }
            else if (f == typeof(EnvironmentController.WLVector2))
            {
                result.Add(colInfo.Name + "X", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "X")));
                result.Add(colInfo.Name + "Y", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Y")));
                return result;
            }
            else if (f == typeof(EnvironmentController.WLVector4))
            {
                result.Add(colInfo.Name + "Red", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Red")));
                result.Add(colInfo.Name + "Green", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Green")));
                result.Add(colInfo.Name + "Blue", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Blue")));
                result.Add(colInfo.Name + "Value", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Value")));
                return result;
            }
            else if (f == typeof(Color))
            {
                result.Add(colInfo.Name + "Red", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Red")));
                result.Add(colInfo.Name + "Green", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Green")));
                result.Add(colInfo.Name + "Blue", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Blue")));
                return result;
            }
            else if (f == typeof(ColorAlpha))
            {
                result.Add(colInfo.Name + "Red", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Red")));
                result.Add(colInfo.Name + "Green", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Green")));
                result.Add(colInfo.Name + "Blue", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Blue")));
                result.Add(colInfo.Name + "Alpha", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Alpha")));
                return result;
            }
            else
            {
                result.Add(colInfo.Name, string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name)));
                return result;
            }
        }

        public static Dictionary<string, string> AddDefault(this IColumnInfo colInfo)
        {
            var result = new Dictionary<string, string>();
            var t = new NpgsqlCommandBuilder();
            Type f = colInfo.FieldType;
            if (f == typeof(Vector3))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a Vector3 for field " + colInfo.Name);
                    }

                    var v = (Vector3)colInfo.Default;
                    result.Add(colInfo.Name + "X", string.Format(CultureInfo.InvariantCulture, "ALTER {1} SET DEFAULT '{0}'", v.X, t.QuoteIdentifier(colInfo.Name + "X")));
                    result.Add(colInfo.Name + "Y", string.Format(CultureInfo.InvariantCulture, "ALTER {1} SET DEFAULT '{0}'", v.Y, t.QuoteIdentifier(colInfo.Name + "Y")));
                    result.Add(colInfo.Name + "Z", string.Format(CultureInfo.InvariantCulture, "ALTER {1} SET DEFAULT '{0}'", v.Z, t.QuoteIdentifier(colInfo.Name + "Z")));
                }
                return result;
            }
            else if (f == typeof(GridVector))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a GridVector for field " + colInfo.Name);
                    }

                    var v = (GridVector)colInfo.Default;
                    result.Add(colInfo.Name + "X", string.Format(CultureInfo.InvariantCulture, "ALTER {1} SET DEFAULT '{0}'", v.X, t.QuoteIdentifier(colInfo.Name + "X")));
                    result.Add(colInfo.Name + "Y", string.Format(CultureInfo.InvariantCulture, "ALTER {1} SET DEFAULT '{0}'", v.Y, t.QuoteIdentifier(colInfo.Name + "Y")));
                }
                return result;
            }
            else if (f == typeof(Vector4))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a Vector4 for field " + colInfo.Name);
                    }

                    var v = (Vector4)colInfo.Default;
                    result.Add(colInfo.Name + "X", string.Format(CultureInfo.InvariantCulture, "ALTER {1} SET DEFAULT '{0}'", v.X, t.QuoteIdentifier(colInfo.Name + "X")));
                    result.Add(colInfo.Name + "Y", string.Format(CultureInfo.InvariantCulture, "ALTER {1} SET DEFAULT '{0}'", v.Y, t.QuoteIdentifier(colInfo.Name + "Y")));
                    result.Add(colInfo.Name + "Z", string.Format(CultureInfo.InvariantCulture, "ALTER {1} SET DEFAULT '{0}'", v.Z, t.QuoteIdentifier(colInfo.Name + "Z")));
                    result.Add(colInfo.Name + "W", string.Format(CultureInfo.InvariantCulture, "ALTER {1} SET DEFAULT '{0}'", v.W, t.QuoteIdentifier(colInfo.Name + "W")));
                }
                return result;
            }
            else if (f == typeof(Quaternion))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a Quaternion for " + colInfo.Name);
                    }

                    var v = (Quaternion)colInfo.Default;
                    result.Add(colInfo.Name + "X", string.Format(CultureInfo.InvariantCulture, "ALTER {1} SET DEFAULT '{0}'", v.X, t.QuoteIdentifier(colInfo.Name + "X")));
                    result.Add(colInfo.Name + "Y", string.Format(CultureInfo.InvariantCulture, "ALTER {1} SET DEFAULT '{0}'", v.Y, t.QuoteIdentifier(colInfo.Name + "Y")));
                    result.Add(colInfo.Name + "Z", string.Format(CultureInfo.InvariantCulture, "ALTER {1} SET DEFAULT '{0}'", v.Z, t.QuoteIdentifier(colInfo.Name + "Z")));
                    result.Add(colInfo.Name + "W", string.Format(CultureInfo.InvariantCulture, "ALTER {1} SET DEFAULT '{0}'", v.W, t.QuoteIdentifier(colInfo.Name + "W")));
                }
                return result;
            }
            else if (f == typeof(EnvironmentController.WLVector2))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a EnvironmentController.WLVector2 for field " + colInfo.Name);
                    }

                    var v = (EnvironmentController.WLVector2)colInfo.Default;
                    result.Add(colInfo.Name + "X", string.Format("ALTER {1} SET DEFAULT '{0}'", v.X, t.QuoteIdentifier(colInfo.Name + "X")));
                    result.Add(colInfo.Name + "Y", string.Format("ALTER {1} SET DEFAULT '{0}'", v.Y, t.QuoteIdentifier(colInfo.Name + "Y")));
                }
                return result;
            }
            else if (f == typeof(EnvironmentController.WLVector4))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a EnvironmentController.WLVector4 for field " + colInfo.Name);
                    }

                    var v = (EnvironmentController.WLVector4)colInfo.Default;
                    result.Add(colInfo.Name + "Red", string.Format("ALTER {1} SET DEFAULT '{0}'", v.X, t.QuoteIdentifier(colInfo.Name + "Red")));
                    result.Add(colInfo.Name + "Green", string.Format("ALTER {1} SET DEFAULT '{0}'", v.Y, t.QuoteIdentifier(colInfo.Name + "Green")));
                    result.Add(colInfo.Name + "Blue", string.Format("ALTER {1} SET DEFAULT '{0}'", v.Z, t.QuoteIdentifier(colInfo.Name + "Blue")));
                    result.Add(colInfo.Name + "Value", string.Format("ALTER {1} SET DEFAULT '{0}'", v.W, t.QuoteIdentifier(colInfo.Name + "Value")));
                }
                return result;
            }
            else if (f == typeof(Color))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a Color for field " + colInfo.Name);
                    }

                    var v = (Color)colInfo.Default;
                    result.Add(colInfo.Name + "Red", string.Format("ALTER {1} SET DEFAULT '{0}'", v.R, t.QuoteIdentifier(colInfo.Name + "Red")));
                    result.Add(colInfo.Name + "Green", string.Format("ALTER {1} SET DEFAULT '{0}'", v.G, t.QuoteIdentifier(colInfo.Name + "Green")));
                    result.Add(colInfo.Name + "Blue", string.Format("ALTER {1} SET DEFAULT '{0}'", v.B, t.QuoteIdentifier(colInfo.Name + "Blue")));
                }
                return result;
            }
            else if (f == typeof(ColorAlpha))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a ColorAlpha for field " + colInfo.Name);
                    }

                    var v = (ColorAlpha)colInfo.Default;
                    result.Add(colInfo.Name + "Red", string.Format("ALTER {1} SET DEFAULT '{0}'", v.R, t.QuoteIdentifier(colInfo.Name + "Red")));
                    result.Add(colInfo.Name + "Green", string.Format("ALTER {1} SET DEFAULT '{0}'", v.G, t.QuoteIdentifier(colInfo.Name + "Green")));
                    result.Add(colInfo.Name + "Blue", string.Format("ALTER {1} SET DEFAULT '{0}'", v.B, t.QuoteIdentifier(colInfo.Name + "Blue")));
                    result.Add(colInfo.Name + "Alpha", string.Format("ALTER {1} SET DEFAULT '{0}'", v.A, t.QuoteIdentifier(colInfo.Name + "Alpha")));
                }
                return result;
            }

            if (colInfo.Default != null && !colInfo.IsNullAllowed)
            {
                if (colInfo.Default.GetType() != colInfo.FieldType &&
                    !(colInfo.Default.GetType() == typeof(UUID) &&
                    colInfo.FieldType == typeof(UGUI)) &&
                    !(colInfo.Default.GetType() == typeof(UUID) &&
                    colInfo.FieldType == typeof(UGUIWithName)) &&
                    !(colInfo.Default.GetType() == typeof(UUID) &&
                    colInfo.FieldType == typeof(UGI)) &&
                    !(colInfo.Default.GetType() == typeof(UUID) &&
                    colInfo.FieldType == typeof(UEI)))
                {
                    throw new ArgumentOutOfRangeException("Default does not match expected type in field " + colInfo.Name + " target type=" + colInfo.FieldType.FullName + " defaultType=" + colInfo.Default.GetType().FullName);
                }

                object def = colInfo.Default;
                if (typeof(bool) == f)
                {
                    def = ((bool)def) ? 1 : 0;
                }
                else if (typeof(Date) == f)
                {
                    def = ((Date)def).AsULong;
                }
                else if (typeof(ParcelID) == f)
                {
                    def = (Guid)new UUID(((ParcelID)def).GetBytes(), 0);
                }
                else if (f.IsEnum)
                {
                    def = Convert.ChangeType(def, f.GetEnumUnderlyingType());
                }
                result.Add(colInfo.Name, string.Format("ALTER {1} SET DEFAULT {0}", def.ToString().ToNpgsqlQuoted(), t.QuoteIdentifier(colInfo.Name)));
            }

            return result;
        }

        public static Dictionary<string, string> DropNotNull(this IColumnInfo colInfo)
        {
            var result = new Dictionary<string, string>();
            var t = new NpgsqlCommandBuilder();
            Type f = colInfo.FieldType;
            string cmdgen = "ALTER {0} DROP NOT NULL";

            if (f == typeof(Vector3))
            {
                result.Add(colInfo.Name + "X", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "X")));
                result.Add(colInfo.Name + "Y", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Y")));
                result.Add(colInfo.Name + "Z", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Z")));
                return result;
            }
            else if (f == typeof(GridVector))
            {
                result.Add(colInfo.Name + "X", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "X")));
                result.Add(colInfo.Name + "Y", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Y")));
                return result;
            }
            else if (f == typeof(Vector4))
            {
                result.Add(colInfo.Name + "X", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "X")));
                result.Add(colInfo.Name + "Y", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Y")));
                result.Add(colInfo.Name + "Z", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Z")));
                result.Add(colInfo.Name + "W", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "W")));
                return result;
            }
            else if (f == typeof(Quaternion))
            {
                result.Add(colInfo.Name + "X", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "X")));
                result.Add(colInfo.Name + "Y", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Y")));
                result.Add(colInfo.Name + "Z", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Z")));
                result.Add(colInfo.Name + "W", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "W")));
                return result;
            }
            else if (f == typeof(EnvironmentController.WLVector2))
            {
                result.Add(colInfo.Name + "X", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "X")));
                result.Add(colInfo.Name + "Y", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Y")));
                return result;
            }
            else if (f == typeof(EnvironmentController.WLVector4))
            {
                result.Add(colInfo.Name + "Red", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Red")));
                result.Add(colInfo.Name + "Green", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Green")));
                result.Add(colInfo.Name + "Blue", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Blue")));
                result.Add(colInfo.Name + "Value", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Value")));
                return result;
            }
            else if (f == typeof(Color))
            {
                result.Add(colInfo.Name + "Red", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Red")));
                result.Add(colInfo.Name + "Green", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Green")));
                result.Add(colInfo.Name + "Blue", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Blue")));
                return result;
            }
            else if (f == typeof(ColorAlpha))
            {
                result.Add(colInfo.Name + "Red", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Red")));
                result.Add(colInfo.Name + "Green", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Green")));
                result.Add(colInfo.Name + "Blue", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Blue")));
                result.Add(colInfo.Name + "Alpha", string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name + "Alpha")));
                return result;
            }
            else
            {
                result.Add(colInfo.Name, string.Format(cmdgen, t.QuoteIdentifier(colInfo.Name)));
                return result;
            }
        }

        public static Dictionary<string, string> AddNotNull(this IColumnInfo colInfo)
        {
            var result = new Dictionary<string, string>();
            var t = new NpgsqlCommandBuilder();
            Type f = colInfo.FieldType;
            if (f == typeof(Vector3))
            {
                if (!colInfo.IsNullAllowed)
                {
                    result.Add(colInfo.Name + "X", $"ALTER {t.QuoteIdentifier(colInfo.Name + "X")} SET NOT NULL");
                    result.Add(colInfo.Name + "Y", $"ALTER {t.QuoteIdentifier(colInfo.Name + "Y")} SET NOT NULL");
                    result.Add(colInfo.Name + "Z", $"ALTER {t.QuoteIdentifier(colInfo.Name + "Z")} SET NOT NULL");
                }
                return result;
            }
            else if (f == typeof(GridVector))
            {
                if (!colInfo.IsNullAllowed)
                {
                    result.Add(colInfo.Name + "X", $"ALTER {t.QuoteIdentifier(colInfo.Name + "X")} SET NOT NULL");
                    result.Add(colInfo.Name + "Y", $"ALTER {t.QuoteIdentifier(colInfo.Name + "Y")} SET NOT NULL");
                }
                return result;
            }
            else if (f == typeof(Vector4))
            {
                if (!colInfo.IsNullAllowed)
                {
                    result.Add(colInfo.Name + "X", $"ALTER {t.QuoteIdentifier(colInfo.Name + "X")} SET NOT NULL");
                    result.Add(colInfo.Name + "Y", $"ALTER {t.QuoteIdentifier(colInfo.Name + "Y")} SET NOT NULL");
                    result.Add(colInfo.Name + "Z", $"ALTER {t.QuoteIdentifier(colInfo.Name + "Z")} SET NOT NULL");
                    result.Add(colInfo.Name + "W", $"ALTER {t.QuoteIdentifier(colInfo.Name + "W")} SET NOT NULL");
                }
                return result;
            }
            else if (f == typeof(Quaternion))
            {
                if (!colInfo.IsNullAllowed)
                {
                    result.Add(colInfo.Name + "X", $"ALTER {t.QuoteIdentifier(colInfo.Name + "X")} SET NOT NULL");
                    result.Add(colInfo.Name + "Y", $"ALTER {t.QuoteIdentifier(colInfo.Name + "Y")} SET NOT NULL");
                    result.Add(colInfo.Name + "Z", $"ALTER {t.QuoteIdentifier(colInfo.Name + "Z")} SET NOT NULL");
                    result.Add(colInfo.Name + "W", $"ALTER {t.QuoteIdentifier(colInfo.Name + "W")} SET NOT NULL");
                }
                return result;
            }
            else if (f == typeof(EnvironmentController.WLVector2))
            {
                if (!colInfo.IsNullAllowed)
                {
                    result.Add(colInfo.Name + "X", $"ALTER {t.QuoteIdentifier(colInfo.Name + "X")} SET NOT NULL");
                    result.Add(colInfo.Name + "Y", $"ALTER {t.QuoteIdentifier(colInfo.Name + "Y")} SET NOT NULL");
                }
                return result;
            }
            else if (f == typeof(EnvironmentController.WLVector4))
            {
                if (!colInfo.IsNullAllowed)
                {
                    result.Add(colInfo.Name + "Red", $"ALTER {t.QuoteIdentifier(colInfo.Name + "Red")} SET NOT NULL");
                    result.Add(colInfo.Name + "Green", $"ALTER {t.QuoteIdentifier(colInfo.Name + "Green")} SET NOT NULL");
                    result.Add(colInfo.Name + "Blue", $"ALTER {t.QuoteIdentifier(colInfo.Name + "Blue")} NOT NULL");
                    result.Add(colInfo.Name + "Value", $"ALTER {t.QuoteIdentifier(colInfo.Name + "Value")} SET NOT NULL");
                }
                return result;
            }
            else if (f == typeof(Color))
            {
                if (!colInfo.IsNullAllowed)
                {
                    result.Add(colInfo.Name + "Red", $"ALTER {t.QuoteIdentifier(colInfo.Name + "Red")} SET NOT NULL");
                    result.Add(colInfo.Name + "Green", $"ALTER {t.QuoteIdentifier(colInfo.Name + "Green")} SET NOT NULL");
                    result.Add(colInfo.Name + "Blue", $"ALTER {t.QuoteIdentifier(colInfo.Name + "Blue")} SET NOT NULL");
                }
                return result;
            }
            else if (f == typeof(ColorAlpha))
            {
                if (!colInfo.IsNullAllowed)
                {
                    result.Add(colInfo.Name + "Red", $"ALTER {t.QuoteIdentifier(colInfo.Name + "Red")} SET NOT NULL");
                    result.Add(colInfo.Name + "Green", $"ALTER {t.QuoteIdentifier(colInfo.Name + "Green")} SET NOT NULL");
                    result.Add(colInfo.Name + "Blue", $"ALTER {t.QuoteIdentifier(colInfo.Name + "Blue")} SET NOT NULL");
                    result.Add(colInfo.Name + "Alpha", $"ALTER {t.QuoteIdentifier(colInfo.Name + "Alpha")} SET NOT NULL");
                }
                return result;
            }

            if (colInfo.Default != null && !colInfo.IsNullAllowed)
            {
                result.Add(colInfo.Name, $"ALTER {t.QuoteIdentifier(colInfo.Name)} SET NOT NULL");
            }

            return result;
        }

        public static Dictionary<string, string> ColumnSql(this IColumnInfo colInfo, bool useDefaultAndNotNull = true)
        {
            var result = new Dictionary<string, string>();
            string notNull = colInfo.IsNullAllowed || !useDefaultAndNotNull ? string.Empty : "NOT NULL ";
            string typeSql;
            Type f = colInfo.FieldType;
            if (f == typeof(string))
            {
                typeSql = (colInfo.Cardinality == 0) ?
                    "text" :
                    (colInfo.IsFixed ? "CHAR" : "VARCHAR") + "(" + colInfo.Cardinality.ToString() + ")";
            }
            else if (f == typeof(UGUI) || f == typeof(UGUIWithName) || f == typeof(UGI) || f == typeof(UEI))
            {
                typeSql = "VARCHAR(255)";
            }
            else if (f == typeof(UUID) || f == typeof(ParcelID))
            {
                typeSql = "uuid";
            }
            else if (f == typeof(double))
            {
                typeSql = "float8";
            }
            else if (f.IsEnum)
            {
                Type enumType = f.GetEnumUnderlyingType();
                if (enumType == typeof(ulong) || enumType == typeof(long))
                {
                    typeSql = "bigint";
                }
                else if (enumType == typeof(byte) || enumType == typeof(ushort) || enumType == typeof(sbyte) || enumType == typeof(short))
                {
                    typeSql = "smallint";
                }
                else if (enumType == typeof(uint))
                {
                    typeSql = "integer";
                }
                else
                {
                    typeSql = "integer";
                }
            }
            else if (f == typeof(int) || f == typeof(uint))
            {
                typeSql = "integer";
            }
            else if (f == typeof(short) || f == typeof(ushort) || f == typeof(byte) || f == typeof(sbyte))
            {
                typeSql = "smallint";
            }
            else if (f == typeof(bool))
            {
                typeSql = "bool";
            }
            else if (f == typeof(long) || f == typeof(ulong) || f == typeof(Date))
            {
                typeSql = "bigint";
            }
            else if (f == typeof(Vector3))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed && useDefaultAndNotNull)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a Vector3 for field " + colInfo.Name);
                    }

                    var v = (Vector3)colInfo.Default;
                    result.Add(colInfo.Name + "X", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.X));
                    result.Add(colInfo.Name + "Y", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.Y));
                    result.Add(colInfo.Name + "Z", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.Z));
                }
                else
                {
                    result.Add(colInfo.Name + "X", "float8 " + notNull);
                    result.Add(colInfo.Name + "Y", "float8 " + notNull);
                    result.Add(colInfo.Name + "Z", "float8 " + notNull);
                }
                return result;
            }
            else if (f == typeof(GridVector))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed && useDefaultAndNotNull)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a GridVector for field " + colInfo.Name);
                    }

                    var v = (GridVector)colInfo.Default;
                    result.Add(colInfo.Name + "X", string.Format("integer {0} DEFAULT '{1}'", notNull, v.X));
                    result.Add(colInfo.Name + "Y", string.Format("integer {0} DEFAULT '{1}'", notNull, v.Y));
                }
                else
                {
                    result.Add(colInfo.Name + "X", "integer " + notNull);
                    result.Add(colInfo.Name + "Y", "integer " + notNull);
                }
                return result;
            }
            else if (f == typeof(Vector4))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed && useDefaultAndNotNull)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a Vector4 for field " + colInfo.Name);
                    }

                    var v = (Vector4)colInfo.Default;
                    result.Add(colInfo.Name + "X", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.X));
                    result.Add(colInfo.Name + "Y", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.Y));
                    result.Add(colInfo.Name + "Z", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.Z));
                    result.Add(colInfo.Name + "W", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.W));
                }
                else
                {
                    result.Add(colInfo.Name + "X", "float8 " + notNull);
                    result.Add(colInfo.Name + "Y", "float8 " + notNull);
                    result.Add(colInfo.Name + "Z", "float8 " + notNull);
                    result.Add(colInfo.Name + "W", "float8 " + notNull);
                }
                return result;
            }
            else if (f == typeof(Quaternion))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed && useDefaultAndNotNull)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a Quaternion for " + colInfo.Name);
                    }

                    var v = (Quaternion)colInfo.Default;
                    result.Add(colInfo.Name + "X", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.X));
                    result.Add(colInfo.Name + "Y", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.Y));
                    result.Add(colInfo.Name + "Z", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.Z));
                    result.Add(colInfo.Name + "W", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.W));
                }
                else
                {
                    result.Add(colInfo.Name + "X", "float8 " + notNull);
                    result.Add(colInfo.Name + "Y", "float8 " + notNull);
                    result.Add(colInfo.Name + "Z", "float8 " + notNull);
                    result.Add(colInfo.Name + "W", "float8 " + notNull);
                }
                return result;
            }
            else if (f == typeof(EnvironmentController.WLVector2))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed && useDefaultAndNotNull)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a EnvironmentController.WLVector2 for field " + colInfo.Name);
                    }

                    var v = (EnvironmentController.WLVector2)colInfo.Default;
                    result.Add(colInfo.Name + "X", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.X));
                    result.Add(colInfo.Name + "Y", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.Y));
                }
                else
                {
                    result.Add(colInfo.Name + "X", "float8 " + notNull);
                    result.Add(colInfo.Name + "Y", "float8 " + notNull);
                }
                return result;
            }
            else if (f == typeof(EnvironmentController.WLVector4))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed && useDefaultAndNotNull)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a EnvironmentController.WLVector4 for field " + colInfo.Name);
                    }

                    var v = (EnvironmentController.WLVector4)colInfo.Default;
                    result.Add(colInfo.Name + "Red", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.X));
                    result.Add(colInfo.Name + "Green", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.Y));
                    result.Add(colInfo.Name + "Blue", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.Z));
                    result.Add(colInfo.Name + "Value", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.W));
                }
                else
                {
                    result.Add(colInfo.Name + "Red", "float8 " + notNull);
                    result.Add(colInfo.Name + "Green", "float8 " + notNull);
                    result.Add(colInfo.Name + "Blue", "float8 " + notNull);
                    result.Add(colInfo.Name + "Value", "float8 " + notNull);
                }
                return result;
            }
            else if (f == typeof(Color))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed && useDefaultAndNotNull)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a Color for field " + colInfo.Name);
                    }

                    var v = (Color)colInfo.Default;
                    result.Add(colInfo.Name + "Red", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.R));
                    result.Add(colInfo.Name + "Green", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.G));
                    result.Add(colInfo.Name + "Blue", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.B));
                }
                else
                {
                    result.Add(colInfo.Name + "Red", "float8 " + notNull);
                    result.Add(colInfo.Name + "Green", "float8 " + notNull);
                    result.Add(colInfo.Name + "Blue", "float8 " + notNull);
                }
                return result;
            }
            else if (f == typeof(ColorAlpha))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed && useDefaultAndNotNull)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a ColorAlpha for field " + colInfo.Name);
                    }

                    var v = (ColorAlpha)colInfo.Default;
                    result.Add(colInfo.Name + "Red", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.R));
                    result.Add(colInfo.Name + "Green", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.G));
                    result.Add(colInfo.Name + "Blue", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.B));
                    result.Add(colInfo.Name + "Alpha", string.Format("float8 {0} DEFAULT '{1}'", notNull, v.A));
                }
                else
                {
                    result.Add(colInfo.Name + "Red", "float8 " + notNull);
                    result.Add(colInfo.Name + "Green", "float8 " + notNull);
                    result.Add(colInfo.Name + "Blue", "float8 " + notNull);
                    result.Add(colInfo.Name + "Alpha", "float8 " + notNull);
                }
                return result;
            }
            else if (f == typeof(byte[]))
            {
                typeSql = "BYTEA";
            }
            else
            {
                throw new ArgumentOutOfRangeException("FieldType " + f.FullName + " is not supported in field " + colInfo.Name);
            }

            if (colInfo.Default != null && !colInfo.IsNullAllowed && useDefaultAndNotNull)
            {
                if(colInfo.Default.GetType() != colInfo.FieldType &&
                    !(colInfo.Default.GetType() == typeof(UUID) &&
                    colInfo.FieldType == typeof(UGUI)) &&
                    !(colInfo.Default.GetType() == typeof(UUID) &&
                    colInfo.FieldType == typeof(UGUIWithName)) &&
                    !(colInfo.Default.GetType() == typeof(UUID) &&
                    colInfo.FieldType == typeof(UGI)) &&
                    !(colInfo.Default.GetType() == typeof(UUID) &&
                    colInfo.FieldType == typeof(UEI)))
                {
                    throw new ArgumentOutOfRangeException("Default does not match expected type in field " + colInfo.Name + " target type=" + colInfo.FieldType.FullName + " defaultType=" + colInfo.Default.GetType().FullName);
                }

                object def = colInfo.Default;
                if(typeof(bool) == f)
                {
                    def = ((bool)def) ? 1 : 0;
                }
                else if(typeof(Date) == f)
                {
                    def = ((Date)def).AsULong;
                }
                else if(typeof(ParcelID) == f)
                {
                    def = (Guid)new UUID(((ParcelID)def).GetBytes(), 0);
                }
                else if(f.IsEnum)
                {
                    def = Convert.ChangeType(def, f.GetEnumUnderlyingType());
                }
                result.Add(colInfo.Name, string.Format("{0} {1} DEFAULT {2}",
                    typeSql,
                    notNull,
                    def.ToString().ToNpgsqlQuoted()));
            }
            else
            {
                result.Add(colInfo.Name, typeSql + " " + notNull);
            }
            return result;
        }
    }

    public class AddColumn<T> : IMigrationElement, IAddColumn
    {
        public string Name { get; }

        public Type FieldType => typeof(T);

        public uint Cardinality { get; set; }

        public bool IsNullAllowed { get; set; }
        public bool IsLong { get; set;  }
        public bool IsFixed { get; set; }

        public object Default { get; set; }

        public AddColumn(string name)
        {
            Name = name;
            IsLong = false;
            IsNullAllowed = true;
            Default = default(T);
        }

        public string FieldSql()
        {
            var parts = new List<string>();
            NpgsqlCommandBuilder b = new NpgsqlCommandBuilder();
            foreach(KeyValuePair<string, string> kvp in this.ColumnSql())
            {
                parts.Add(b.QuoteIdentifier(kvp.Key) + " " + kvp.Value);
            }
            return string.Join(",", parts);
        }

        public string AlterFieldSql()
        {
            var parts = new List<string>();
            NpgsqlCommandBuilder b = new NpgsqlCommandBuilder();
            foreach (KeyValuePair<string, string> kvp in this.ColumnSql())
            {
                parts.Add(b.QuoteIdentifier(kvp.Key) + " " + kvp.Value);
            }
            return string.Join(", ADD COLUMN ", parts);
        }

        public string Sql(string tableName) => string.Format("ALTER TABLE {0} ADD COLUMN {1}", new NpgsqlCommandBuilder().QuoteIdentifier(tableName), AlterFieldSql());
    }

    public class DropColumn : IMigrationElement
    {
        public string Name { get; private set; }
        public DropColumn(string name)
        {
            Name = name;
        }

        public string Sql(string tableName)
        {
            throw new NotSupportedException();
        }

        public string Sql(string tableName, Type formerType)
        {
            var fieldNames = new string[] { Name };

            if (formerType == typeof(Vector3))
            {
                fieldNames = new string[]
                {
                    Name + "X", Name + "Y", Name + "Z"
                };
            }
            else if (formerType == typeof(GridVector))
            {
                fieldNames = new string[]
                {
                    Name + "X", Name + "Y"
                };
            }
            else if (formerType == typeof(Vector4) || formerType == typeof(Quaternion))
            {
                fieldNames = new string[]
                {
                    Name + "X", Name + "Y", Name + "Z", Name + "W"
                };
            }
            else if (formerType == typeof(EnvironmentController.WLVector4))
            {
                fieldNames = new string[]
                {
                    Name + "Red", Name + "Green", Name + "Blue", Name + "Value"
                };
            }
            else if (formerType == typeof(Color))
            {
                fieldNames = new string[]
                {
                    Name + "Red", Name + "Green", Name + "Blue"
                };
            }
            else if (formerType == typeof(ColorAlpha))
            {
                fieldNames = new string[]
                {
                    Name + "Red", Name + "Green", Name + "Blue", Name + "Alpha"
                };
            }

            NpgsqlCommandBuilder b = new NpgsqlCommandBuilder();
            for(int i = 0; i < fieldNames.Length; ++i)
            {
                fieldNames[i] = $"DROP COLUMN {b.QuoteIdentifier(fieldNames[i])}";
            }
            return $"ALTER TABLE {b.QuoteIdentifier(tableName)} {string.Join(",", fieldNames)};";
        }
    }

    public interface IChangeColumn : IColumnInfo
    {
        string Sql(string tableName, Type formerType);
        string OldName { get; }
    }

    class FormerFieldInfo : IColumnInfo
    {
        readonly IColumnInfo m_ColumnInfo;
        public FormerFieldInfo(IColumnInfo columnInfo, Type oldFieldType)
        {
            FieldType = oldFieldType;
            m_ColumnInfo = columnInfo;
        }

        public uint Cardinality { get { return 0; } }
        public object Default { get { return null; } }
        public Type FieldType { get; }
        public bool IsNullAllowed { get { return true; } }
        public bool IsLong { get { return m_ColumnInfo.IsLong; } }
        public bool IsFixed { get { return m_ColumnInfo.IsFixed;  } }

        public string Name { get { return m_ColumnInfo.Name; } }
        public string FieldSql()
        {
            throw new NotSupportedException();
        }
    }

    public class ChangeColumn<T> : IMigrationElement, IChangeColumn
    {
        public string Name { get; }
        public string OldName { get; set; }
        public Type FieldType => typeof(T);

        public bool IsNullAllowed { get; set; }
        public bool IsLong { get; set; }
        public bool IsFixed { get; set; }
        public uint Cardinality { get; set; }
        public bool FixedLength { get; set; }
        public object Default { get; set; }

        public ChangeColumn(string name)
        {
            Name = name;
            OldName = name;
        }

        public string FieldSql()
        {
            var parts = new List<string>();
            NpgsqlCommandBuilder b = new NpgsqlCommandBuilder();
            foreach (KeyValuePair<string, string> kvp in this.ColumnSql())
            {
                parts.Add(b.QuoteIdentifier(kvp.Key) + " " + kvp.Value);
            }
            return string.Join(",", parts);
        }

        public string Sql(string tableName)
        {
            throw new NotSupportedException();
        }

        public string Sql(string tableName, Type formerType)
        {
            var oldField = new FormerFieldInfo(this, formerType);
            List<string> oldFields;
            Dictionary<string, string> newFields;
            Dictionary<string, string> newFieldDefaults;
            Dictionary<string, string> newFieldNotNulls;
            Dictionary<string, string> newFieldDropDefaults;
            Dictionary<string, string> newFieldDropNotNulls;

            oldFields = new List<string>(oldField.ColumnSql().Keys);
            newFields = this.ColumnSql(false);
            newFieldDropDefaults = this.DropDefault();
            newFieldDefaults = this.AddDefault();
            newFieldDropNotNulls = this.DropNotNull();
            newFieldNotNulls = this.AddNotNull();

            var sqlParts = new List<string>();
            var sqlAttrs = new List<string>();
            var sqlRenames = new List<string>();
            NpgsqlCommandBuilder b = new NpgsqlCommandBuilder();

            /* remove anything that is not needed anymore */
            foreach (string fieldName in oldFields)
            {
                if (!newFields.ContainsKey(fieldName))
                {
                    sqlParts.Add("DROP COLUMN " + b.QuoteIdentifier(fieldName));
                }
            }

            foreach(KeyValuePair<string, string> kvp in newFields)
            {
                string sqlPart = string.Empty;
                if(oldFields.Contains(kvp.Key))
                {
                    string oldName = OldName + kvp.Key.Substring(Name.Length);
                    if (oldName != kvp.Key)
                    {
                        sqlRenames.Add($"RENAME COLUMN {b.QuoteIdentifier(oldName)} TO {b.QuoteIdentifier(kvp.Key)}");
                    }
                    string drop;
                    if(newFieldDropDefaults.TryGetValue(kvp.Key, out drop))
                    {
                        sqlPart += drop + ",";
                    }
                    sqlPart += $"ALTER {b.QuoteIdentifier(kvp.Key)} TYPE";
                }
                else
                {
                    newFieldDropDefaults.Remove(kvp.Key);
                    newFieldDropNotNulls.Remove(kvp.Key);
                    sqlPart = "ADD " + b.QuoteIdentifier(kvp.Key);
                }
                sqlPart += " " + kvp.Value;
                sqlParts.Add(sqlPart);
                string def;
                if (newFieldDefaults.TryGetValue(kvp.Key, out def))
                {
                    sqlAttrs.Add(def);
                }
                if (newFieldNotNulls.TryGetValue(kvp.Key, out def))
                {
                    sqlAttrs.Add(def);
                }
            }

            sqlParts.AddRange(sqlAttrs);
            string renameInst = string.Empty;
            if(sqlRenames.Count != 0)
            {
                renameInst = "ALTER TABLE " + b.QuoteIdentifier(tableName) + " " + string.Join(",", sqlRenames) + ";";
            }
            return renameInst + "ALTER TABLE " + b.QuoteIdentifier(tableName) + " " + string.Join(",", sqlParts) + ";";
        }
    }
    #endregion

    public class TableRevision : IMigrationElement
    {
        public uint Revision { get; }

        public TableRevision(uint revision)
        {
            Revision = revision;
        }

        public string Sql(string tableName) => string.Format("COMMENT ON TABLE {0} COMMENT='{1}'", new NpgsqlCommandBuilder().QuoteIdentifier(tableName), Revision);
    }

    public class SqlStatement : IMigrationElement
    {
        public string Statement { get; }

        public SqlStatement(string statement)
        {
            Statement = statement;
        }

        public string Sql(string tableName) => Statement;
    }
}
