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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.kite.KiteColumnHandle;
import com.facebook.presto.plugin.kite.KiteTableProperties;
import com.vitessedata.kite.sdk.CsvFileSpec;
import com.vitessedata.kite.sdk.FileSpec;
import com.vitessedata.kite.sdk.ParquetFileSpec;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;

public final class KiteSqlUtils
{
    private static final Logger log = Logger.get(KiteSqlUtils.class);

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
        else if (type.equals(DATE)) {
            return "DATE " + quoteStringLiteral(value);
        }
        else if (type.equals(TIME)) {
            return "TIME " + quoteStringLiteral(value);
        }
        else if (type.equals(TIMESTAMP) || type.equals(TIMESTAMP_MICROSECONDS)) {
            return "TIMESTAMP " + quoteStringLiteral(value);
        }
        else {
            return value;
        }
    }

    public static String toSQLCompatibleString(Object value, Type type)
    {
        if (type.equals(DATE)) {
            if (value instanceof Long) {
                long ts = ((Long) value).longValue();
                ts *= 24 * 3600000L;
                Date date = new Date(ts);
                return date.toString();
            }
            else {
                throw new IllegalStateException("DateType but value is not Long." + value.getClass().getName());
            }
        }
        else if (type.equals(TIMESTAMP) || type.equals(TIMESTAMP_MICROSECONDS)) {
            if (value instanceof Long) {
                long v = ((Long) value).longValue();
                if (type.equals(TIMESTAMP_MICROSECONDS)) {
                    v /= 1000;
                }
                Timestamp ts = new Timestamp(v);
                return ts.toString();
            }
            else {
                throw new IllegalStateException("TimestampType but value is not Long." + value.getClass().getName());
            }
        }
        else if (type.equals(TIME)) {
            if (value instanceof Long) {
                Time tm = new Time(((Long) value).longValue());
                return tm.toString();
            }
            else {
                throw new IllegalStateException("TimeType but value is not Long." + value.getClass().getName());
            }
        }
        else if (type instanceof DecimalType) {
            DecimalType dect = (DecimalType) type;
            if (value instanceof Long) {
                BigDecimal dec = new BigDecimal(((Long) value).longValue()).movePointLeft(dect.getScale());
                return dec.toString();
            }
            else {
                throw new IllegalStateException("TimeType but value is not Long." + value.getClass().getName());
            }
        }

        if (value instanceof Slice) {
            return ((Slice) value).toStringUtf8();
        }
        return value.toString();
    }

    private static String getKiteType(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return "int8:0:0";
        }
        else if (type.equals(TINYINT)) {
            return "int8:0:0";
        }
        else if (type.equals(SMALLINT)) {
            return "int16:0:0";
        }
        else if (type.equals(INTEGER)) {
            return "int32:0:0";
        }
        else if (type.equals(BIGINT)) {
            return "int64:0:0";
        }
        else if (type.equals(REAL)) {
            return "fp32:0:0";
        }
        else if (type.equals(DOUBLE)) {
            return "fp64:0:0";
        }
        else if (type.equals(DATE)) {
            return "date:0:0";
        }
        else if (type.equals(TIME) || type.equals(TIME_WITH_TIME_ZONE)) {
            return "time:0:0";
        }
        else if (type.equals(TIMESTAMP) || type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            return "timestamp:0:0";
        }
        else if (type.equals(TIMESTAMP_MICROSECONDS)) {
            return "timestamp:0:0";
        }
        else if (type instanceof DecimalType) {
            DecimalType dec = (DecimalType) type;
            int precision = dec.getPrecision();
            int scale = dec.getScale();
            return "decimal:" + precision + ":" + scale;
        }
        else if (type instanceof CharType || type instanceof VarcharType) {
            return "string:0:0";
        }
        else if (type.equals(VARCHAR)) {
            return "string:0:0";
        }
        else if (type.equals(VARBINARY)) {
            return "bytea:0:0";
        }
        else {
            throw new NotSupportedException("type not supported in Kite. " + type.getClass().getName());
        }
    }

    public static String createSchema(List<KiteColumnHandle> columns)
    {
        StringBuilder sb = new StringBuilder();

        for (KiteColumnHandle c : columns) {
            String cname = c.getName();
            Type type = c.getColumnType();
            int precision = 0;
            int scale = 0;
            sb.append(cname);
            sb.append(':');
            sb.append(getKiteType(type));
            sb.append('\n');
        }

        return sb.toString();
    }

    public static String createSQL(List<KiteColumnHandle> columns, String path, String whereClause)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        String colstr = columns.stream().map(c -> c.getName()).collect(Collectors.joining(", "));
        sb.append(colstr);
        sb.append(" FROM ");
        sb.append(quoteIdentifier(path));
        if (whereClause != null && whereClause.length() > 0) {
            sb.append(" WHERE ");
            sb.append(whereClause);
        }

        return sb.toString();
    }

    // TODO: get CSV options
    public static FileSpec createFileSpec(Map<String, Object> properties)
    {
        String format = (String) properties.get("format");
        if (format.equalsIgnoreCase("csv")) {
            char quote = KiteTableProperties.getCsvQuote(properties);
            char escape = KiteTableProperties.getCsvEscape(properties);
            char sep = KiteTableProperties.getCsvSeparator(properties);
            boolean header = KiteTableProperties.getCsvHeader(properties);
            String nullstr = KiteTableProperties.getCsvNullString(properties);
            return new CsvFileSpec().quote(quote).escape(escape).delim(sep).header_line(header).nullstr(nullstr);
        }
        else if (format.equalsIgnoreCase("parquet")) {
            return new ParquetFileSpec();
        }
        else {
            throw new NotSupportedException("format not supported in Kite. " + format);
        }
    }

    public static String getPreferredHost(String[] hosts, int fragid)
    {
        int idx = fragid % hosts.length;
        return hosts[idx];
    }

    public static KiteSqlUtils.KiteURL toURL(String location)
    {
        KiteSqlUtils.KiteURL url = new KiteSqlUtils.KiteURL(location);
        return url;
    }

    public static class KiteURL
    {
        private final String location;
        private final String[] hosts;
        private final String path;

        public KiteURL(String location)
        {
            this.location = location;
            if (!location.startsWith("kite://")) {
                throw new IllegalArgumentException("invalid location. " + location);
            }

            String[] parts = location.substring(7).split("/", 2);
            if (parts.length != 2) {
                throw new IllegalArgumentException("invalid location. " + location);
            }

            hosts = parts[0].split(",");
            path = parts[1];
        }

        public String getPath()
        {
            return path;
        }

        public String[] getHosts()
        {
            return hosts;
        }
    }
}
