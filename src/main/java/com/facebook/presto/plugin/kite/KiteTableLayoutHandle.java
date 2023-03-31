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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class KiteTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final KiteTableHandle table;
    private final Optional<List<ColumnHandle>> desiredColumns;
    private final List<KiteDataFragment> dataFragments;

    @JsonCreator
    public KiteTableLayoutHandle(
            @JsonProperty("table") KiteTableHandle table,
            @JsonProperty("desiredColumns") Optional<List<ColumnHandle>> desiredColumns,
            @JsonProperty("dataFragments") List<KiteDataFragment> dataFragments)
    {
        this.table = requireNonNull(table, "table is null");
        this.desiredColumns = requireNonNull(desiredColumns, "desireColumns is null");
        this.dataFragments = requireNonNull(dataFragments, "dataFragments is null");
    }

    @JsonProperty
    public KiteTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public List<KiteDataFragment> getDataFragments()
    {
        return dataFragments;
    }

    @JsonProperty
    public Optional<List<ColumnHandle>> getDesiredColumns()
    {
        return desiredColumns;
    }

    public String getConnectorId()
    {
        return table.getConnectorId();
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
