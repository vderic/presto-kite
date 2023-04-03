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
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class KiteConnector
        implements Connector
{
    private static final Logger log = Logger.get(KiteConnector.class);

    private final KiteMetadata metadata;
    private final KiteSplitManager splitManager;
    private final KiteRecordSetProvider recordSetProvider;
    private final KitePageSinkProvider pageSinkProvider;
    private final KiteTableProperties kiteTableProperties;
    private List<PropertyMetadata<?>> tableProperties;

    @Inject
    public KiteConnector(
            KiteMetadata metadata,
            KiteSplitManager splitManager,
            KiteRecordSetProvider recordSetProvider,
            KitePageSinkProvider pageSinkProvider,
            KiteTableProperties kiteTableProperties)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.kiteTableProperties = requireNonNull(kiteTableProperties, "kiteTableProperties is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return KiteTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        if (tableProperties == null) {
            tableProperties = ImmutableList.copyOf(kiteTableProperties.getTableProperties());
        }
        return tableProperties;
    }
}
