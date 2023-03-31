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
import com.facebook.presto.common.Page;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageSinkContext;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class KitePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private static final Logger log = Logger.get(KitePageSinkProvider.class);

    private final KitePagesStore pagesStore;
    private final HostAddress currentHostAddress;

    @Inject
    public KitePageSinkProvider(KitePagesStore pagesStore, NodeManager nodeManager)
    {
        this(pagesStore, requireNonNull(nodeManager, "nodeManager is null").getCurrentNode().getHostAndPort());
    }

    @VisibleForTesting
    public KitePageSinkProvider(KitePagesStore pagesStore, HostAddress currentHostAddress)
    {
        this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
        this.currentHostAddress = requireNonNull(currentHostAddress, "currentHostAddress is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, PageSinkContext pageSinkContext)
    {
        log.info("createPageSink output");
        checkArgument(!pageSinkContext.isCommitRequired(), "Kite connector does not support page sink commit");

        KiteOutputTableHandle kiteOutputTableHandle = (KiteOutputTableHandle) outputTableHandle;
        KiteTableHandle tableHandle = kiteOutputTableHandle.getTable();
        long tableId = tableHandle.getTableId();
        checkState(kiteOutputTableHandle.getActiveTableIds().contains(tableId));

        pagesStore.cleanUp(kiteOutputTableHandle.getActiveTableIds());
        pagesStore.initialize(tableId);
        return new KitePageSink(pagesStore, currentHostAddress, tableId);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, PageSinkContext pageSinkContext)
    {
        log.info("createPageSink insert");
        checkArgument(!pageSinkContext.isCommitRequired(), "Kite connector does not support page sink commit");

        KiteInsertTableHandle kiteInsertTableHandle = (KiteInsertTableHandle) insertTableHandle;
        KiteTableHandle tableHandle = kiteInsertTableHandle.getTable();
        long tableId = tableHandle.getTableId();
        checkState(kiteInsertTableHandle.getActiveTableIds().contains(tableId));

        pagesStore.cleanUp(kiteInsertTableHandle.getActiveTableIds());
        pagesStore.initialize(tableId);
        return new KitePageSink(pagesStore, currentHostAddress, tableId);
    }

    private static class KitePageSink
            implements ConnectorPageSink
    {
        private final KitePagesStore pagesStore;
        private final HostAddress currentHostAddress;
        private final long tableId;
        private long addedRows;

        public KitePageSink(KitePagesStore pagesStore, HostAddress currentHostAddress, long tableId)
        {
            this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
            this.currentHostAddress = requireNonNull(currentHostAddress, "currentHostAddress is null");
            this.tableId = tableId;
        }

        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            pagesStore.add(tableId, page);
            addedRows += page.getPositionCount();
            return NOT_BLOCKED;
        }

        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            return completedFuture(ImmutableList.of(new KiteDataFragment(currentHostAddress, addedRows).toSlice()));
        }

        @Override
        public void abort()
        {
        }
    }
}
