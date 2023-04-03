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

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class KiteTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final KiteTableHandle table;
    private final String whereClause;

    @JsonCreator
    public KiteTableLayoutHandle(
            @JsonProperty("table") KiteTableHandle table,
            @JsonProperty("whereClause") String whereClause)
    {
        this.table = requireNonNull(table, "table is null");
        this.whereClause = requireNonNull(whereClause, "whereClause is null");
    }

    @JsonProperty
    public KiteTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public String getWhereClause()
    {
        return whereClause;
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
