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
    public static final String CSV_SEPARATOR = "csv_separator";
    public static final String CSV_QUOTE = "csv_quote";
    public static final String CSV_ESCAPE = "csv_escape";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public KiteTableProperties()
    {
        tableProperties = ImmutableList.of(
                stringProperty(STORAGE_FORMAT_PROPERTY, "Kite storager format for the table", null, false),
                stringProperty(LOCATION_PROPERTY, "kite://host/path", null, false),
                stringProperty(CSV_SEPARATOR, "CSV separator character", null, false),
                stringProperty(CSV_QUOTE, "CSV quote character", null, false),
                stringProperty(CSV_ESCAPE, "CSV escape character", null, false));
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

    public static String getCsvSeparator(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(CSV_SEPARATOR);
    }

    public static String getCsvQuote(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(CSV_QUOTE);
    }

    public static String getCsvEscape(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(CSV_ESCAPE);
    }

    public static void show(Map<String, Object> tableProperties)
    {
        for (Map.Entry<String, Object> e : tableProperties.entrySet()) {
            log.info(e.getKey() + ": " + e.getValue());
        }
    }
}
