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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Set;

public final class KiteSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(KiteSplitManager.class);

    private final NodeManager nodeManager;
    private final int splitsPerNode;

    @Inject
    public KiteSplitManager(NodeManager nodeManager, KiteConfig config)
    {
        this.nodeManager = nodeManager;
        this.splitsPerNode = config.getSplitsPerNode();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layoutHandle,
            SplitSchedulingContext splitSchedulingContext)
    {
        KiteTableLayoutHandle layout = (KiteTableLayoutHandle) layoutHandle;

        List<KiteDataFragment> dataFragments = layout.getDataFragments();

        log.info("KiteSplitManager: #worker" + nodeManager.getRequiredWorkerNodes().size());
        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();
        for (Node node : nodes) {
            log.info("NODE: " + node.toString());
        }
        log.info("getSplits: #datafragment = " + dataFragments.size() + ", #splitsPerNode=" + splitsPerNode);
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        int fragid = 0;
        int fragcnt = dataFragments.size() * splitsPerNode;
        for (KiteDataFragment dataFragment : dataFragments) {
            for (int i = 0; i < splitsPerNode; i++) {
                splits.add(
                        new KiteSplit(
                                layout.getTable(),
                                fragid++,
                                fragcnt,
                                layout.getWhereClause(),
                                dataFragment.getHostAddress(),
                                i,
                                splitsPerNode,
                                dataFragment.getRows()));
            }
        }
        return new FixedSplitSource(splits.build());
    }
}
