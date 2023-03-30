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
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.ViewNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

@ThreadSafe
public class KiteMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(KiteMetadata.class);

    public static final String SCHEMA_NAME = "default";

    private final NodeManager nodeManager;
    private final String connectorId;
    private final List<String> schemas = new ArrayList<>();
    private final AtomicLong nextTableId = new AtomicLong();
    private final Map<SchemaTableName, Long> tableIds = new HashMap<>();
    private final Map<Long, KiteTableHandle> tables = new HashMap<>();
    private final Map<Long, Map<HostAddress, KiteDataFragment>> tableDataFragments = new HashMap<>();
    private final Map<SchemaTableName, String> views = new HashMap<>();

    @Inject
    public KiteMetadata(NodeManager nodeManager, KiteConnectorId connectorId)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.schemas.add(SCHEMA_NAME);
    }

    @Override
    public synchronized List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(schemas);
    }

    @Override
    public synchronized void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        if (schemas.contains(schemaName)) {
            throw new PrestoException(ALREADY_EXISTS, format("Schema [%s] already exists", schemaName));
        }
        schemas.add(schemaName);
    }

    @Override
    public synchronized void dropSchema(ConnectorSession session, String schemaName)
    {
        if (!schemas.contains(schemaName)) {
            throw new PrestoException(NOT_FOUND, format("Schema [%s] does not exist", schemaName));
        }

        boolean tablesExist = tables.values().stream()
                .anyMatch(table -> table.getSchemaName().equals(schemaName));

        if (tablesExist) {
            throw new PrestoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }

        verify(schemas.remove(schemaName));
    }

    @Override
    public synchronized ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Long tableId = tableIds.get(schemaTableName);
        if (tableId == null) {
            return null;
        }
        return tables.get(tableId);
    }

    @Override
    public synchronized ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        KiteTableHandle kiteTableHandle = (KiteTableHandle) tableHandle;
        return kiteTableHandle.toTableMetadata();
    }

    @Override
    public synchronized List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return tables.values().stream()
                .filter(table -> schemaNameOrNull == null || table.getSchemaName().equals(schemaNameOrNull))
                .map(KiteTableHandle::toSchemaTableName)
                .collect(toList());
    }

    @Override
    public synchronized Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        KiteTableHandle kiteTableHandle = (KiteTableHandle) tableHandle;
        return kiteTableHandle.getColumnHandles().stream()
                .collect(toMap(KiteColumnHandle::getName, Function.identity()));
    }

    @Override
    public synchronized ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        KiteColumnHandle kiteColumnHandle = (KiteColumnHandle) columnHandle;
        return kiteColumnHandle.toColumnMetadata();
    }

    @Override
    public synchronized Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return tables.values().stream()
                .filter(table -> prefix.matches(table.toSchemaTableName()))
                .collect(toMap(KiteTableHandle::toSchemaTableName, handle -> handle.toTableMetadata().getColumns()));
    }

    @Override
    public synchronized void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        KiteTableHandle handle = (KiteTableHandle) tableHandle;
        Long tableId = tableIds.remove(handle.toSchemaTableName());
        if (tableId != null) {
            tables.remove(tableId);
            tableDataFragments.remove(tableId);
        }
    }

    @Override
    public synchronized void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        checkSchemaExists(newTableName.getSchemaName());
        checkTableNotExists(newTableName);
        KiteTableHandle oldTableHandle = (KiteTableHandle) tableHandle;
        KiteTableHandle newTableHandle = new KiteTableHandle(
                oldTableHandle.getConnectorId(),
                newTableName.getSchemaName(),
                newTableName.getTableName(),
                oldTableHandle.getTableId(),
                oldTableHandle.getColumnHandles());
        tableIds.remove(oldTableHandle.toSchemaTableName());
        tableIds.put(newTableName, oldTableHandle.getTableId());
        tables.remove(oldTableHandle.getTableId());
        tables.put(oldTableHandle.getTableId(), newTableHandle);
    }

    @Override
    public synchronized void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        ConnectorOutputTableHandle outputTableHandle = beginCreateTable(session, tableMetadata, Optional.empty());
        finishCreateTable(session, outputTableHandle, ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public synchronized KiteOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        checkSchemaExists(tableMetadata.getTable().getSchemaName());
        checkTableNotExists(tableMetadata.getTable());
        long nextId = nextTableId.getAndIncrement();
        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();
        checkState(!nodes.isEmpty(), "No Kite nodes available");

        KiteTableProperties.show(tableMetadata.getProperties());
        tableIds.put(tableMetadata.getTable(), nextId);
        KiteTableHandle table = new KiteTableHandle(
                connectorId,
                nextId,
                tableMetadata);
        tables.put(table.getTableId(), table);
        tableDataFragments.put(table.getTableId(), new HashMap<>());

        return new KiteOutputTableHandle(table, ImmutableSet.copyOf(tableIds.values()));
    }

    private void checkSchemaExists(String schemaName)
    {
        if (!schemas.contains(schemaName)) {
            throw new SchemaNotFoundException(schemaName);
        }
    }

    private void checkTableNotExists(SchemaTableName tableName)
    {
        if (tables.values().stream()
                .map(KiteTableHandle::toSchemaTableName)
                .anyMatch(tableName::equals)) {
            throw new PrestoException(ALREADY_EXISTS, format("Table [%s] already exists", tableName.toString()));
        }
        if (views.keySet().contains(tableName)) {
            throw new PrestoException(ALREADY_EXISTS, format("View [%s] already exists", tableName.toString()));
        }
    }

    @Override
    public synchronized Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        KiteOutputTableHandle kiteOutputHandle = (KiteOutputTableHandle) tableHandle;

        updateRowsOnHosts(kiteOutputHandle.getTable(), fragments);
        return Optional.empty();
    }

    @Override
    public synchronized KiteInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        KiteTableHandle kiteTableHandle = (KiteTableHandle) tableHandle;
        return new KiteInsertTableHandle(kiteTableHandle, ImmutableSet.copyOf(tableIds.values()));
    }

    @Override
    public synchronized Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        requireNonNull(insertHandle, "insertHandle is null");
        KiteInsertTableHandle kiteInsertHandle = (KiteInsertTableHandle) insertHandle;

        updateRowsOnHosts(kiteInsertHandle.getTable(), fragments);
        return Optional.empty();
    }

    @Override
    public synchronized void createView(ConnectorSession session, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        SchemaTableName viewName = viewMetadata.getTable();
        checkSchemaExists(viewName.getSchemaName());
        if (getTableHandle(session, viewName) != null) {
            throw new PrestoException(ALREADY_EXISTS, "Table already exists: " + viewName);
        }

        if (replace) {
            views.put(viewName, viewData);
        }
        else if (views.putIfAbsent(viewName, viewData) != null) {
            throw new PrestoException(ALREADY_EXISTS, "View already exists: " + viewName);
        }
    }

    @Override
    public synchronized void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        if (views.remove(viewName) == null) {
            throw new ViewNotFoundException(viewName);
        }
    }

    @Override
    public synchronized List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return views.keySet().stream()
                .filter(viewName -> (schemaNameOrNull == null) || schemaNameOrNull.equals(viewName.getSchemaName()))
                .collect(toImmutableList());
    }

    @Override
    public synchronized Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return views.entrySet().stream()
                .filter(entry -> prefix.matches(entry.getKey()))
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> new ConnectorViewDefinition(entry.getKey(), Optional.empty(), entry.getValue())));
    }

    private void updateRowsOnHosts(KiteTableHandle table, Collection<Slice> fragments)
    {
        checkState(
                tableDataFragments.containsKey(table.getTableId()),
                "Uninitialized table [%s.%s]",
                table.getSchemaName(),
                table.getTableName());
        Map<HostAddress, KiteDataFragment> dataFragments = tableDataFragments.get(table.getTableId());

        for (Slice fragment : fragments) {
            KiteDataFragment kiteDataFragment = KiteDataFragment.fromSlice(fragment);
            dataFragments.merge(kiteDataFragment.getHostAddress(), kiteDataFragment, KiteDataFragment::merge);
        }
    }

    @Override
    public synchronized List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle handle,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        requireNonNull(handle, "handle is null");
        checkArgument(handle instanceof KiteTableHandle);
        KiteTableHandle kiteTableHandle = (KiteTableHandle) handle;
        checkState(
                tableDataFragments.containsKey(kiteTableHandle.getTableId()),
                "Inconsistent state for the table [%s.%s]",
                kiteTableHandle.getSchemaName(),
                kiteTableHandle.getTableName());

        List<KiteDataFragment> expectedFragments = ImmutableList.copyOf(
                tableDataFragments.get(kiteTableHandle.getTableId()).values());

        KiteTableLayoutHandle layoutHandle = new KiteTableLayoutHandle(kiteTableHandle, expectedFragments);
        log.info(constraint.getSummary().toString());
        return ImmutableList.of(new ConnectorTableLayoutResult(getTableLayout(session, layoutHandle), constraint.getSummary()));
    }

    @Override
    public synchronized ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(
                handle,
                Optional.empty(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
    }
}
