/*
 * Copyright 2022-2025 Crown Copyright
 *
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
package sleeper.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTablePartitioning;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.SortingProperty;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ComputedStatistics;

import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.range.Range;
import sleeper.core.schema.Schema;
import sleeper.trino.handle.SleeperColumnHandle;
import sleeper.trino.handle.SleeperInsertTableHandle;
import sleeper.trino.handle.SleeperPartitioningHandle;
import sleeper.trino.handle.SleeperTableHandle;
import sleeper.trino.remotesleeperconnection.SleeperConnectionAsTrino;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * Provides information to Trino about the table structure and other metadata. This class also handles the application
 * of static filters to tables, and the resolution of indexes.
 */
public class SleeperMetadata implements ConnectorMetadata {
    private static final Logger LOGGER = Logger.get(SleeperMetadata.class);

    private final SleeperConfig sleeperConfig;
    private final SleeperConnectionAsTrino sleeperConnectionAsTrino;

    @Inject
    public SleeperMetadata(SleeperConfig sleeperConfig,
            SleeperConnectionAsTrino sleeperConnectionAsTrino) {
        this.sleeperConfig = requireNonNull(sleeperConfig);
        this.sleeperConnectionAsTrino = requireNonNull(sleeperConnectionAsTrino);
    }

    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Suppress warning for now, method can probably be deleted
    private static List<String> handleToNames(List<ColumnHandle> columnHandles) {
        return columnHandles.stream()
                .map(SleeperColumnHandle.class::cast)
                .map(SleeperColumnHandle::getColumnName)
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * List all of the schemas in the underlying Sleeper database.
     *
     * @param  session the current session (makes no difference at present)
     * @return         a list of all the names of the schemas
     */
    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return sleeperConnectionAsTrino.getAllTrinoSchemaNames();
    }

    /**
     * List all of the tables in a schema.
     *
     * @param  session            the current session (makes no difference at present)
     * @param  trinoSchemaNameOpt the schema to examine, or the default schema if this is not set
     * @return                    a list of {@link SchemaTableName} objects
     */
    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> trinoSchemaNameOpt) {
        String trinoSchemaName = trinoSchemaNameOpt.orElse(sleeperConnectionAsTrino.getDefaultTrinoSchemaName());
        return sleeperConnectionAsTrino.getAllSchemaTableNamesInTrinoSchema(trinoSchemaName).stream()
                .sorted(Comparator.comparing(SchemaTableName::getSchemaName).thenComparing(SchemaTableName::getTableName))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Retrieve a table handle for a specified schema and table name.
     *
     * @param  session         the current session (makes no difference at present)
     * @param  schemaTableName the schema and table name
     * @return                 the handle for the table
     */
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName) {
        return sleeperConnectionAsTrino.getSleeperTableHandle(schemaTableName);
    }

    /**
     * Retrieve the metadata for the specified table.
     *
     * @param  session              the current session (makes no difference at present)
     * @param  connectorTableHandle the handle for the table to be examined
     * @return                      the metadata for the table
     */
    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle connectorTableHandle) {
        SleeperTableHandle sleeperTableHandle = (SleeperTableHandle) connectorTableHandle;
        return sleeperTableHandle.toConnectorTableMetadata();
    }

    /**
     * Retrieve columns metadata for tables matching a supplied prefix. This seems to have something to do with
     * redirected tables.
     *
     * @param  session the current session (makes no difference at present)
     * @param  prefix  the prefix to use to filter the tables
     * @return         an iterator of {@link TableColumnsMetadata} objects
     */
    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        return listTables(session, prefix.getSchema()).stream()
                .filter(prefix::matches)
                .map(schemaTableName -> (SleeperTableHandle) this.getTableHandle(session, schemaTableName))
                .map(SleeperTableHandle::toTableColumnsMetadata)
                .iterator();
    }

    /**
     * Retrieve the column handles for a specified table.
     *
     * @param  session              the current session (makes no difference at present)
     * @param  connectorTableHandle the table to examine
     * @return                      a map where the keys are the column names and the values are the corresponding
     *                              column handles
     */
    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle connectorTableHandle) {
        SleeperTableHandle sleeperTableHandle = (SleeperTableHandle) connectorTableHandle;
        return sleeperTableHandle.getSleeperColumnHandleListInOrder().stream()
                .collect(ImmutableMap.toImmutableMap(SleeperColumnHandle::getColumnName, Function.identity()));
    }

    /**
     * Retrieve the metadata for the specified column of the specified table.
     *
     * @param  session               the current session (makes no difference at present)
     * @param  connectorTableHandle  the table
     * @param  connectorColumnHandle the column
     * @return                       the metadata for the specified table and column
     */
    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            ColumnHandle connectorColumnHandle) {
        SleeperColumnHandle sleeperColumnHandle = (SleeperColumnHandle) connectorColumnHandle;
        return sleeperColumnHandle.toColumnMetadata();
    }

    /**
     * Retrive the properties of the specified table. The properties that are currently returned include the {@link
     * SleeperPartitioningHandle} which describes how the table is partitioned, and a list of {@link SortingProperty}
     * objects which describe the key-0based sort order.
     * <p>
     * At the moment, every Sleeper partition translates into its own Trino partition, but if this becomes too many
     * partitions to pass around, it should be possible to combine consecutive Sleeper partitions into a single Trino
     * partition. The {@link ConnectorTableProperties} class has a range of other features which are not used here, such
     * as a predicate which applies to the whole table.
     * <p>
     * The configuration parameter {@link SleeperConfig#isEnableTrinoPartitioning()} can be used to turn off
     * partitioning.
     *
     * @param  session     the current session (makes no difference at present)
     * @param  tableHandle the table to examine
     * @return             the {@link ConnectorTableProperties} object
     */
    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle) {
        if (sleeperConfig.isEnableTrinoPartitioning()) {
            SleeperTableHandle sleeperTableHandle = (SleeperTableHandle) tableHandle;
            List<ColumnHandle> keyColumnHandles = sleeperTableHandle.getColumnHandlesInCategoryInOrder(SleeperColumnHandle.SleeperColumnCategory.ROWKEY).stream()
                    .map(ColumnHandle.class::cast)
                    .collect(ImmutableList.toImmutableList());

            Schema tableSchema = sleeperConnectionAsTrino.getSchema(sleeperTableHandle.getSchemaTableName());
            // Generate as list of the lower-bounds of each partition and use these to create a SleeperPartitioningHandle object.
            List<Key> partitionMinKeys = sleeperConnectionAsTrino.streamPartitions(sleeperTableHandle.getSchemaTableName())
                    .filter(Partition::isLeafPartition)
                    .map(partition -> Key.create(partition.getRegion().getRangesOrdered(tableSchema).stream().map(Range::getMin).collect(ImmutableList.toImmutableList())))
                    .collect(ImmutableList.toImmutableList());
            ConnectorTablePartitioning connectorTablePartitioning = new ConnectorTablePartitioning(
                    new SleeperPartitioningHandle(sleeperTableHandle, partitionMinKeys), keyColumnHandles);
            // The local properties reflect that Sleeper stores its keys in sort-order. There are no nulls in Sleeper,
            // but Trino only has an ASC_NULLS_FIRST (or last) option.
            List<LocalProperty<ColumnHandle>> localProperties = keyColumnHandles.stream()
                    .map(columnHandle -> new SortingProperty<>(columnHandle, SortOrder.ASC_NULLS_FIRST))
                    .collect(ImmutableList.toImmutableList());
            return new ConnectorTableProperties(
                    TupleDomain.all(),
                    Optional.of(connectorTablePartitioning),
                    Optional.empty(),
                    localProperties);
        } else {
            return new ConnectorTableProperties();
        }
    }

    /**
     * Apply a static filter to a table. This is the mechanism where predicates can be pushed down by the Trino
     * framework into an underlying connector.
     * <p>
     * The method is called repeatedly during the query-optimisation phase and so some of the filters that are passed
     * may never make it into the final execution.
     * <p>
     * This method is passed a {@link SleeperTableHandle} which contains the {@link TupleDomain} that is currently
     * applied to the table, plus an additional {@link Constraint} to try to apply to the table. If this method can push
     * down the supplied constraint then it returns a {@link ConstraintApplicationResult} object specifying the new,
     * filtered, table handle and which parts of the constraint have not been pushed down.
     * <p>
     * This implementation only considers the {@link TupleDomain} part of any {@link Constraint}. The only filters that
     * are pushed down are those which apply to row keys.
     *
     * @param  session              the current session (makes no difference at present)
     * @param  connectorTableHandle the table to apply the filter to
     * @param  additionalConstraint the additional constraint to try to apply to the table
     * @return                      If any part of the constraint can be pushed down, then a
     *                              {@link ConstraintApplicationResult} is returned with the new table handle and the
     *                              remaining parts of the constraint which have not been applied. An empty result
     *                              indicates that this filter could not be pushed down.
     */
    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle connectorTableHandle, Constraint additionalConstraint) {
        SleeperTableHandle sleeperTableHandle = (SleeperTableHandle) connectorTableHandle;
        LOGGER.debug("applyFilter on %s: %s", sleeperTableHandle.getSchemaTableName(), additionalConstraint.getSummary().getDomains());

        Set<SleeperColumnHandle> rowKeyColumnHandlesSet = ImmutableSet.copyOf(
                sleeperTableHandle.getColumnHandlesInCategoryInOrder(SleeperColumnHandle.SleeperColumnCategory.ROWKEY));

        Optional<Map<ColumnHandle, Domain>> additionalConstraintColumnHandleToDomainMapOpt = additionalConstraint.getSummary().getDomains();
        if (additionalConstraintColumnHandleToDomainMapOpt.isEmpty()) {
            LOGGER.debug("No domains were provided in the constraint");
            return Optional.empty();
        }
        Map<ColumnHandle, Domain> additionalConstraintColumnHandleToDomainMap = additionalConstraintColumnHandleToDomainMapOpt.get();

        Map<ColumnHandle, Domain> rowKeyConstraintsColumnHandleToDomainMap = additionalConstraintColumnHandleToDomainMap.entrySet().stream()
                .filter(entry -> rowKeyColumnHandlesSet.contains((SleeperColumnHandle) entry.getKey()))
                .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        if (rowKeyConstraintsColumnHandleToDomainMap.isEmpty()) {
            LOGGER.debug("No row key domains were provided in the constraint");
            return Optional.empty();
        }
        TupleDomain<ColumnHandle> rowKeyConstraintsTupleDomain = TupleDomain.withColumnDomains(rowKeyConstraintsColumnHandleToDomainMap);

        Map<ColumnHandle, Domain> remainingConstraintsColumnHandleToDomainMap = additionalConstraintColumnHandleToDomainMap.entrySet().stream()
                .filter(entry -> !rowKeyColumnHandlesSet.contains((SleeperColumnHandle) entry.getKey()))
                .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        TupleDomain<ColumnHandle> remainingConstraintsTupleDomain = TupleDomain.withColumnDomains(remainingConstraintsColumnHandleToDomainMap);

        TupleDomain<ColumnHandle> originalTableTupleDomain = sleeperTableHandle.getTupleDomain();
        TupleDomain<ColumnHandle> rowKeyConstrainedTableTupleDomain = originalTableTupleDomain.intersect(rowKeyConstraintsTupleDomain);
        if (originalTableTupleDomain.equals(rowKeyConstrainedTableTupleDomain)) {
            LOGGER.debug("New row key domains did not change the overall tuple domain");
            return Optional.empty();
        }

        LOGGER.debug("New domain is %s", rowKeyConstrainedTableTupleDomain);
        LOGGER.debug("Remaining domain is %s", remainingConstraintsTupleDomain);
        return Optional.of(new ConstraintApplicationResult<>(sleeperTableHandle.withTupleDomain(rowKeyConstrainedTableTupleDomain),
                remainingConstraintsTupleDomain,
                false));
    }

    /**
     * Begin an INSERT statement to add rows to a table.
     *
     * @param  session     the session to perform this action under
     * @param  tableHandle the table to add the rows to
     * @param  columns     The columns which are to be added. Null or default columns are not permitted and so these
     *                     columns must match the columns in the table.
     * @param  retryMode   the retry mode (only NO_RETRIES is supported)
     * @return             a handle to this insert operation
     */
    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            RetryMode retryMode) {
        if (retryMode != RetryMode.NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }
        List<SleeperColumnHandle> sleeperColumnHandlesInOrder = columns.stream()
                .map(SleeperColumnHandle.class::cast)
                .collect(ImmutableList.toImmutableList());
        return new SleeperInsertTableHandle((SleeperTableHandle) tableHandle, sleeperColumnHandlesInOrder);
    }

    /**
     * Finish the INSERT operation. In this implementation, the {@link SleeperPageSink} does all of the work and this
     * method does nothing.
     *
     * @param  session            the session to perform this action under
     * @param  insertHandle       the handle of the insert operation
     * @param  fragments          The values which are returned by {@link SleeperPageSink#finish()}, once the future has
     *                            resolved. In this implementation, no values are passed back from the {@link
     *                            SleeperPageSink} to this metadata object.
     * @param  computedStatistics ignored
     * @return                    an empty Optional
     */
    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics) {
        // Do nothing - the records are written when the PageSink is closed
        return Optional.empty();
    }

    @Override
    public ConnectorTableHandle makeCompatiblePartitioning(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorPartitioningHandle partitioningHandle) {
        return tableHandle;
    }
}
