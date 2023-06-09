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
package com.facebook.presto.plugin.kite;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;

public class KiteTableProperties
{
    private static final Logger log = Logger.get(KiteTableProperties.class);

    public static final String STORAGE_FORMAT_PROPERTY = "format";
    public static final String LOCATION_PROPERTY = "location";
    public static final String CSV_DELIM = "csv_delim";
    public static final String CSV_QUOTE = "csv_quote";
    public static final String CSV_ESCAPE = "csv_escape";
    public static final String CSV_HEADER = "csv_header";
    public static final String CSV_NULL_STRING = "csv_nullstr";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public KiteTableProperties(TypeManager typeManager, KiteConfig config)
    {
        tableProperties = ImmutableList.of(
                stringProperty(STORAGE_FORMAT_PROPERTY, "Kite storager format for the table", null, false),
                stringProperty(LOCATION_PROPERTY, "kite://host/path", null, false),
                stringProperty(CSV_DELIM, "CSV delimiter character", null, false),
                stringProperty(CSV_QUOTE, "CSV quote character", null, false),
                stringProperty(CSV_HEADER, "CSV header boolean", null, false),
                stringProperty(CSV_ESCAPE, "CSV escape character", null, false),
                stringProperty(CSV_NULL_STRING, "CSV NULL string", null, false));
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static String getLocation(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(LOCATION_PROPERTY);
    }

    public static String getStorageFormat(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(STORAGE_FORMAT_PROPERTY);
    }

    public static char getCsvDelimiter(Map<String, Object> tableProperties)
    {
        return ((String) tableProperties.getOrDefault(CSV_DELIM, ",")).charAt(0);
    }

    public static char getCsvQuote(Map<String, Object> tableProperties)
    {
        return ((String) tableProperties.getOrDefault(CSV_QUOTE, "\"")).charAt(0);
    }

    public static char getCsvEscape(Map<String, Object> tableProperties)
    {
        return ((String) tableProperties.getOrDefault(CSV_ESCAPE, "\"")).charAt(0);
    }

    public static boolean getCsvHeader(Map<String, Object> tableProperties)
    {
        return ((String) tableProperties.getOrDefault(CSV_HEADER, "false")).equalsIgnoreCase("true");
    }

    public static String getCsvNullString(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.getOrDefault(CSV_NULL_STRING, "");
    }

    public static void show(Map<String, Object> tableProperties)
    {
        for (Map.Entry<String, Object> e : tableProperties.entrySet()) {
            log.info(e.getKey() + ": " + e.getValue());
        }
    }
}
