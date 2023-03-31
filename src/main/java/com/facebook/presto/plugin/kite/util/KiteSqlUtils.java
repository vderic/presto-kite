/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.kite.util;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import io.airlift.slice.Slice;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static java.util.Locale.ENGLISH;

public final class KiteSqlUtils
{
    private KiteSqlUtils()
    {
    }

    private static final String[] KEYWORDS = {"ADD", "ALL", "ALLOW", "ALTER", "AND", "APPLY",
            "ASC", "ASCII", "AUTHORIZE", "BATCH", "BEGIN", "BIGINT", "BLOB", "BOOLEAN", "BY",
            "CLUSTERING", "COLUMNFAMILY", "COMPACT", "COUNT", "COUNTER", "CREATE", "DECIMAL",
            "DATE", "DELETE", "DESC", "DOUBLE", "DROP", "FILTERING", "FLOAT", "FROM", "GRANT", "IN",
            "INDEX", "INET", "INSERT", "INT", "INTO", "KEY", "KEYSPACE", "KEYSPACES", "LIMIT",
            "LIST", "MAP", "MODIFY", "NORECURSIVE", "NOSUPERUSER", "OF", "ON", "ORDER", "PASSWORD",
            "PERMISSION", "PERMISSIONS", "PRIMARY", "RENAME", "REVOKE", "SCHEMA", "SELECT", "SET",
            "SMALLINT", "STORAGE", "SUPERUSER", "TABLE", "TEXT", "TIMESTAMP", "TIMEUUID", "TINYINT", "TO", "TOKEN",
            "TRUNCATE", "TTL", "TYPE", "UNLOGGED", "UPDATE", "USE", "USER", "USERS", "USING",
            "UUID", "VALUES", "VARCHAR", "VARINT", "WHERE", "WITH", "WRITETIME"};

    private static final Set<String> keywords = new HashSet<>(Arrays.asList(KEYWORDS));

    public static final String EMPTY_COLUMN_NAME = "__empty__";

    public static String validTableName(String identifier)
    {
        return validIdentifier(identifier);
    }

    public static String validColumnName(String identifier)
    {
        if (identifier.isEmpty() || identifier.equals(EMPTY_COLUMN_NAME)) {
            return "\"\"";
        }

        return validIdentifier(identifier);
    }

    private static String validIdentifier(String identifier)
    {
        if (!identifier.equals(identifier.toLowerCase(ENGLISH))) {
            return quoteIdentifier(identifier);
        }

        if (keywords.contains(identifier.toUpperCase(ENGLISH))) {
            return quoteIdentifier(identifier);
        }
        return identifier;
    }

    private static String quoteIdentifier(String identifier)
    {
        return '"' + identifier + '"';
    }

    public static String quoteStringLiteral(String string)
    {
        return "'" + string.replace("'", "''") + "'";
    }

    public static String sqlValue(String value, Type type)
    {
        if (type instanceof VarcharType) {
            return quoteStringLiteral(value);
        }
        else {
            return value;
        }
    }

    public static String toSQLCompatibleString(Object value)
    {
        if (value instanceof Slice) {
            return ((Slice) value).toStringUtf8();
        }
        return value.toString();
    }
}
