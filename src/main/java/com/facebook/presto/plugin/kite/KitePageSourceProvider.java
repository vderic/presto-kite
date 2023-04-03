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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.vitessedata.kite.sdk.KiteConnection;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class KitePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger log = Logger.get(KitePageSourceProvider.class);

    private final KitePagesStore pagesStore;

    @Inject
    public KitePageSourceProvider(KitePagesStore pagesStore)
    {
        this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
        KiteConnection kite = new KiteConnection();
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            List<ColumnHandle> columns,
            SplitContext splitContext)
    {
        log.info("create Page Source: split= " + split.toString());
        KiteSplit kiteSplit = (KiteSplit) split;
        long tableId = kiteSplit.getTableHandle().getTableId();
        int fragid = kiteSplit.getFragId();
        int fragcnt = kiteSplit.getFragCnt();
        String whereClause = kiteSplit.getWhereClause();

        for (ColumnHandle c : columns) {
            log.info("PageSource: " + c.toString());
        }

        List<Integer> columnIndexes = columns.stream()
                .map(KiteColumnHandle.class::cast)
                .map(KiteColumnHandle::getColumnIndex).collect(toList());
        List<Page> pages = pagesStore.getPages(
                tableId,
                fragid,
                fragcnt,
                columnIndexes,
                0);

        return new FixedPageSource(pages);
    }
}
