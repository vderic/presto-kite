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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/*
 * TODO: Split shall store the following:
 * - fragid
 * - fragcnt
 * - predicate WHERE clauses
 * - format and its properties csv_quote,...etc.
 * - location kite url
 */
public class KiteSplit
        implements ConnectorSplit
{
    private final KiteTableHandle tableHandle;
    private final int fragId;
    private final int fragCnt;
    private final String whereClause;
    private final HostAddress address;
    private final int partNumber; // part of the pages on one worker that this splits is responsible
    private final int totalPartsPerWorker; // how many concurrent reads there will be from one worker
    private final long expectedRows;

    @JsonCreator
    public KiteSplit(
            @JsonProperty("tableHandle") KiteTableHandle tableHandle,
            @JsonProperty("fragId") int fragId,
            @JsonProperty("fragCnt") int fragCnt,
            @JsonProperty("whereClause") String whereClause,
            @JsonProperty("address") HostAddress address,
            @JsonProperty("partNumber") int partNumber,
            @JsonProperty("totalPartsPerWorker") int totalPartsPerWorker,
            @JsonProperty("expectedRows") long expectedRows)
    {
        checkState(partNumber >= 0, "partNumber must be >= 0");
        checkState(totalPartsPerWorker >= 1, "totalPartsPerWorker must be >= 1");
        checkState(totalPartsPerWorker > partNumber, "totalPartsPerWorker must be > partNumber");
        checkState(fragId >= 0, "fragid must be >= 0");
        checkState(fragId < fragCnt, "fragid must be < fragcnt");

        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.partNumber = partNumber;
        this.totalPartsPerWorker = totalPartsPerWorker;
        this.fragId = fragId;
        this.fragCnt = fragCnt;
        this.whereClause = whereClause;
        this.address = requireNonNull(address, "address is null");
        this.expectedRows = expectedRows;
    }

    @JsonProperty
    public KiteTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public int getFragId()
    {
        return fragId;
    }

    @JsonProperty
    public int getFragCnt()
    {
        return fragCnt;
    }

    @JsonProperty
    public String getWhereClause()
    {
        return whereClause;
    }

    @JsonProperty
    public int getTotalPartsPerWorker()
    {
        return totalPartsPerWorker;
    }

    @JsonProperty
    public int getPartNumber()
    {
        return partNumber;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return HARD_AFFINITY;
    }

    @JsonProperty
    public HostAddress getAddress()
    {
        return address;
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
    {
        return ImmutableList.of(address);
    }

    @JsonProperty
    public long getExpectedRows()
    {
        return expectedRows;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableHandle", tableHandle)
                .add("fragId", fragId)
                .add("fragCnt", fragCnt)
                .add("whereClause", whereClause)
                .add("partNumber", partNumber)
                .add("totalPartsPerWorker", totalPartsPerWorker)
                .toString();
    }
}
