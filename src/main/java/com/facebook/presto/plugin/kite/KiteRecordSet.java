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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.plugin.kite.util.KiteSqlUtils;
import com.facebook.presto.plugin.kite.util.KiteSqlUtils.KiteURL;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;
import com.vitessedata.kite.sdk.FileSpec;
import com.vitessedata.kite.sdk.KiteConnection;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class KiteRecordSet
        implements RecordSet
{
    private static final Logger log = Logger.get(KiteRecordSet.class);

    private final List<KiteColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final KiteConnection kite;

    public KiteRecordSet(KiteSplit split, List<KiteColumnHandle> columnHandles)
    {
        requireNonNull(split, "split is null");

        KiteTableHandle table = split.getTableHandle();
        // fragid and fragcnt
        int fragid = split.getFragId();
        int fragcnt = split.getFragCnt();
        String whereClause = split.getWhereClause();

        // create schema
        List<KiteColumnHandle> fields = table.getColumnHandles();
        String schema = KiteSqlUtils.createSchema(fields);

        // get format, kite URL and fragment
        Map<String, Object> properties = table.getProperties();
        FileSpec filespec = KiteSqlUtils.createFileSpec(properties);

        String format = requireNonNull((String) properties.get("format"), "format is null");
        String location = requireNonNull((String) properties.get("location"), "location is null");

        KiteURL url = KiteSqlUtils.toURL(location);
        String addr = KiteSqlUtils.getPreferredHost(url.getHosts(), fragid);

        // create SQL
        String sql = KiteSqlUtils.createSQL(columnHandles, url.getPath(), whereClause);

        log.info("SQL = " + sql);
        log.info("SCHEMA = " + schema);
        kite = new KiteConnection().host(addr).schema(schema).fragment(fragid, fragcnt).sql(sql).format(filespec);

        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (KiteColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new KiteRecordCursor(kite, columnHandles);
    }
}
