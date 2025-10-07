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
package sleeper.trino.remotesleeperconnection;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Range;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.VarcharType;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.core.partition.Partition;
import sleeper.core.range.Region;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.util.ObjectFactoryException;
import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.trino.SleeperConfig;
import sleeper.trino.handle.SleeperColumnHandle;
import sleeper.trino.handle.SleeperSplit;
import sleeper.trino.handle.SleeperTableHandle;
import sleeper.trino.handle.SleeperTransactionHandle;
import sleeper.trino.utils.SleeperTypeConversionUtils;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * This class works as a translator between Trino concepts and Sleeper concepts. It entirely wraps a {@link
 * SleeperRawAwsConnection} so that the rest of this plugin can access everything it needs without using the underlying
 * raw connection.
 */
public class SleeperConnectionAsTrino implements AutoCloseable {

    private static final String DEFAULT_TRINO_SCHEMA_NAME = "default";
    private final SleeperRawAwsConnection sleeperRawAwsConnection;
    private final Map<String, SleeperTableHandle> tableHandleMap;

    @Inject
    public SleeperConnectionAsTrino(SleeperConfig sleeperConfig,
            S3Client s3Client,
            S3AsyncClient s3AsyncClient,
            HadoopConfigurationProvider hadoopConfigurationProvider,
            DynamoDbClient dynamoDbClient) throws ObjectFactoryException {
        requireNonNull(sleeperConfig);
        this.sleeperRawAwsConnection = new SleeperRawAwsConnection(sleeperConfig, s3Client, s3AsyncClient, hadoopConfigurationProvider, dynamoDbClient);
        this.tableHandleMap = this.sleeperRawAwsConnection.getAllSleeperTableNames().stream()
                .collect(ImmutableMap.toImmutableMap(Function.identity(), this::constructSleeperTableHandle));
    }

    private static SleeperColumnHandle.SleeperColumnCategory convertColumnNameToCategory(
            String columnName,
            Set<String> rowKeyColumnNameSet,
            Set<String> sortKeyColumnNameSet,
            Set<String> valueKeyColumnNameSet) {
        if (rowKeyColumnNameSet.contains(columnName)) {
            return SleeperColumnHandle.SleeperColumnCategory.ROWKEY;
        }
        if (sortKeyColumnNameSet.contains(columnName)) {
            return SleeperColumnHandle.SleeperColumnCategory.SORTKEY;
        }
        if (valueKeyColumnNameSet.contains(columnName)) {
            return SleeperColumnHandle.SleeperColumnCategory.VALUE;
        }
        throw new AssertionError();
    }

    /**
     * Generate column metadata for the system table of partition status. This method is strongly tied to the
     * sleeper.partitions table and so it should probably be moved out of this class and put somewhere else.
     *
     * @return A list of the {@link ColumnMetadata} objects.
     */
    public static List<ColumnMetadata> getPartitionStatusColumnMetadata() {
        return ImmutableList.of(
                ColumnMetadata.builder().setName("schemaname").setType(VarcharType.VARCHAR).setNullable(false).build(),
                ColumnMetadata.builder().setName("tablename").setType(VarcharType.VARCHAR).setNullable(false).build(),
                ColumnMetadata.builder().setName("partitionid").setType(VarcharType.VARCHAR).setNullable(false).build(),
                ColumnMetadata.builder().setName("parentpartitionid").setType(VarcharType.VARCHAR).setNullable(true).build(),
                ColumnMetadata.builder().setName("childpartitionids").setType(new ArrayType(VarcharType.VARCHAR)).setNullable(true).build(),
                ColumnMetadata.builder().setName("minrowkeys").setType(new ArrayType(VarcharType.VARCHAR)).setNullable(true).build(),
                ColumnMetadata.builder().setName("maxrowkeys").setType(new ArrayType(VarcharType.VARCHAR)).setNullable(true).build(),
                ColumnMetadata.builder().setName("dimension").setType(IntegerType.INTEGER).setNullable(false).build(),
                ColumnMetadata.builder().setName("isleafpartition").setType(BooleanType.BOOLEAN).setNullable(false).build());
    }

    private static String nullOrString(Object element) {
        if (element == null) {
            return null;
        } else {
            return element.toString();
        }

    }

    /**
     * Create a new transaction. Transactions are currently referenced by the instant that they start.
     *
     * @return The handle of the new transaction.
     */
    public SleeperTransactionHandle getNewSleeperTransactionHandle() {
        return new SleeperTransactionHandle(Instant.now());
    }

    /**
     * Retrieve all of the Trino schema names. At present, Sleeper does not separate its tables into different schemas
     * and so this method returns the single default schema name.
     *
     * @return A list of Trino schema names.
     */
    public List<String> getAllTrinoSchemaNames() {
        return ImmutableList.of(DEFAULT_TRINO_SCHEMA_NAME);
    }

    /**
     * Retrieve the name of the default schema.
     *
     * @return the name of the default schema
     */
    public String getDefaultTrinoSchemaName() {
        return DEFAULT_TRINO_SCHEMA_NAME;
    }

    /**
     * Retrieve the names of all of the tables in a given Trino schema.
     *
     * @param  trinoSchema the name of the Trino schema to examine
     * @return             a set of {@link SchemaTableName} objects, one for each table in the schema
     */
    public Set<SchemaTableName> getAllSchemaTableNamesInTrinoSchema(String trinoSchema) {
        assert trinoSchema.equals(DEFAULT_TRINO_SCHEMA_NAME);
        return tableHandleMap.keySet().stream()
                .map(tableName -> new SchemaTableName(DEFAULT_TRINO_SCHEMA_NAME, tableName))
                .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * Retrieve a table handle for a given table.
     *
     * @param  schemaTableName the schema and table name to use to create the handle
     * @return                 the {@link SleeperTableHandle} object
     */
    public SleeperTableHandle getSleeperTableHandle(SchemaTableName schemaTableName) {
        assert schemaTableName.getSchemaName().equals(DEFAULT_TRINO_SCHEMA_NAME);
        return tableHandleMap.get(schemaTableName.getTableName());
    }

    /**
     * Retrieve information about every partition of a given table and stream that information back to the caller. The
     * schema of each row that is returned is defined in {@link #getPartitionStatusColumnMetadata()}.
     * <p>
     * This method is strongly tied to the system.partitions table and so it would be good to remove it from this
     * generic Sleeper-as-Trino access class. However, that would mean exposing the Sleeper-specific Partition class
     * outside of this package, and that does not seem like a good idea either.
     *
     * @param  schemaTableName The schema and table names to use to retrieve the partition information.
     * @return                 A stream of rows, each containing the partition information as a list of objects.
     */
    public Stream<List<Object>> streamPartitionStatusRows(SchemaTableName schemaTableName) {
        assert schemaTableName.getSchemaName().equals(DEFAULT_TRINO_SCHEMA_NAME);
        List<Partition> allPartitions = sleeperRawAwsConnection.getSleeperTablePartitionStructure(
                schemaTableName.getTableName(),
                Instant.now()).getAllPartitions();
        Schema schema = sleeperRawAwsConnection.getSleeperSchema(schemaTableName.getTableName());
        // Some of the partition methods can return null and so an ImmutableList cannot be used here
        return allPartitions.stream()
                .map(partition -> Arrays.asList(
                        schemaTableName.getSchemaName(),
                        schemaTableName.getTableName(),
                        partition.getId(),
                        partition.getParentPartitionId(),
                        partition.getChildPartitionIds(),
                        partition.getRegion().getRangesOrdered(schema).stream().map(sleeper.core.range.Range::getMin).map(SleeperConnectionAsTrino::nullOrString).collect(Collectors.toList()),
                        partition.getRegion().getRangesOrdered(schema).stream().map(sleeper.core.range.Range::getMax).map(SleeperConnectionAsTrino::nullOrString).collect(Collectors.toList()),
                        partition.getDimension(),
                        partition.isLeafPartition()));
    }

    /**
     * This exposes too much of the internals of Sleeper - try to find a way around this.
     *
     * @param  schemaTableName the table to stream the partition information from
     * @return                 the stream of partitions
     */
    public Stream<Partition> streamPartitions(SchemaTableName schemaTableName) {
        assert schemaTableName.getSchemaName().equals(DEFAULT_TRINO_SCHEMA_NAME);
        return sleeperRawAwsConnection.getSleeperTablePartitionStructure(
                schemaTableName.getTableName(),
                Instant.now()).getAllPartitions().stream();
    }

    /**
     * Fetch the Sleeper schema for a given table name.
     *
     * @param  schemaTableName the table to get the schema for
     * @return                 the Sleeper table schema
     */
    public Schema getSchema(SchemaTableName schemaTableName) {
        return sleeperRawAwsConnection.getSleeperSchema(schemaTableName.getTableName());
    }

    /**
     * Stream all of the results from a single split object. The split contains the details of the partition, files and
     * all of the row key ranges that are to be scanned.
     *
     * @param  sleeperTransactionHandle          The transaction that these splits will be generated under
     * @param  sleeperSplit                      The split to scan
     * @param  outputSleeperColumnHandlesInOrder The columns to return
     * @return                                   A stream of result rows, each expressed as a List of Objects, in the
     *                                           same order as the output columns
     *                                           specified in the outputSleeperColumnHandlesInOrder argument.
     */
    public Stream<List<Object>> streamEntireSplitResultRows(SleeperTransactionHandle sleeperTransactionHandle,
            SleeperSplit sleeperSplit,
            List<SleeperColumnHandle> outputSleeperColumnHandlesInOrder) {
        // Retrieve the LeafPartitionQuery from the split and then restrict it so that it only returns the
        // requested rows
        List<String> columnNamesInOrder = outputSleeperColumnHandlesInOrder.stream()
                .map(SleeperColumnHandle::getColumnName)
                .collect(ImmutableList.toImmutableList());
        LeafPartitionQuery leafPartitionQuery = sleeperSplit.getLeafPartitionQuery()
                .withRequestedValueFields(columnNamesInOrder);

        // Stream the results, as Record objects, and then convert them into a List<Object>
        try {
            Stream<Row> resultRecordStream = this.sleeperRawAwsConnection.createResultRecordStream(
                    sleeperTransactionHandle.getTransactionStartInstant(),
                    leafPartitionQuery);

            return resultRecordStream
                    .map(record -> outputSleeperColumnHandlesInOrder.stream()
                            .map(sleeperColumnHandle -> record.get(sleeperColumnHandle.getColumnName()))
                            .collect(ImmutableList.toImmutableList()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Produces split objects for a list of ranges. The {@link SleeperSplit} objects can be fully scanned to return the
     * relevant rows. The split contains all of the range information that is needed to complete the scans, and so this
     * method is the point in the execution process where the tupledomain derived from the user's query is converted
     * into something which directly describes how that data will be read from Sleeper.
     * <p>
     * In this implementation, the method {@link SleeperRawAwsConnection#splitIntoLeafPartitionQueries} is used to
     * generate the splits.
     *
     * @param  sleeperTransactionHandle The transaction that these splits will be generated under
     * @param  sleeperTableHandle       The table to generate the splits for
     * @param  trinoRangeList           A list of the ranges to generate the splits for
     * @return                          A list of {@link SleeperSplit} objects generated from the supplied ranges
     */
    public List<SleeperSplit> generateSleeperSplits(
            SleeperTransactionHandle sleeperTransactionHandle, SleeperTableHandle sleeperTableHandle,
            List<Range> trinoRangeList) {
        List<SleeperColumnHandle> rowKeySleeperColumnHandlesInOrder = sleeperTableHandle.getColumnHandlesInCategoryInOrder(SleeperColumnHandle.SleeperColumnCategory.ROWKEY);

        if (rowKeySleeperColumnHandlesInOrder.size() > 1) {
            throw new UnsupportedOperationException("Single-valued rowkeys only");
        }
        SleeperColumnHandle rowKeySleeperColumnHandle = rowKeySleeperColumnHandlesInOrder.get(0);

        // Convert the Trino Range objects into Sleeper Range objects
        Schema sleeperSchema = sleeperRawAwsConnection.getSleeperSchema(sleeperTableHandle.getSchemaTableName().getTableName());
        sleeper.core.range.Range.RangeFactory rangeFactory = new sleeper.core.range.Range.RangeFactory(sleeperSchema);
        List<Region> sleeperRegionList = trinoRangeList.stream().map(
                trinoRange -> rangeFactory.createRange(
                        rowKeySleeperColumnHandle.getColumnName(),
                        SleeperTypeConversionUtils.convertTrinoObjectToSleeperRowKeyObject(
                                rowKeySleeperColumnHandle.getColumnTrinoType(), trinoRange.getLowBoundedValue()),
                        trinoRange.isLowInclusive(),
                        SleeperTypeConversionUtils.convertTrinoObjectToSleeperRowKeyObject(
                                rowKeySleeperColumnHandle.getColumnTrinoType(), trinoRange.getHighBoundedValue()),
                        trinoRange.isHighInclusive()))
                .map(sleeperRange -> new Region(ImmutableList.of(sleeperRange)))
                .collect(ImmutableList.toImmutableList());

        // Construct a Sleeper query with a unique, random query ID
        Query sleeperQuery = Query.builder()
                .tableName(sleeperTableHandle.getSchemaTableName().getTableName())
                .queryId(UUID.randomUUID().toString())
                .regions(sleeperRegionList)
                .build();

        // Split the query into leaf partition queries and return them.
        try {
            List<LeafPartitionQuery> leafPartitionQueryList = this.sleeperRawAwsConnection.splitIntoLeafPartitionQueries(
                    sleeperTransactionHandle.getTransactionStartInstant(),
                    sleeperQuery);
            return leafPartitionQueryList.stream()
                    .map(leafPartitionQuery -> new SleeperSplit(sleeperSchema, leafPartitionQuery))
                    .collect(ImmutableList.toImmutableList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new ingest coordinator to add rows to a table. This pass-through method exposes the internals of the
     * AWS connection and requires revision.
     * <p>
     * Make sure to initialise the returned object and close it after use.
     *
     * @param  schemaTableName The schema and table to add the rows to. The schema must be the Sleeper default schema.
     * @return                 The new {@link IngestCoordinator} object.
     */
    public IngestCoordinator<Page> createIngestCoordinator(SchemaTableName schemaTableName) {
        assert schemaTableName.getSchemaName().equals(DEFAULT_TRINO_SCHEMA_NAME);
        try {
            return this.sleeperRawAwsConnection.createIngestRowsAsync(schemaTableName.getTableName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Close the underlying connection.
     */
    @Override
    public void close() {
        this.sleeperRawAwsConnection.close();
    }

    private SleeperTableHandle constructSleeperTableHandle(String tableName) {
        Schema sleeperSchema = this.sleeperRawAwsConnection.getSleeperSchema(tableName);
        List<Field> allFieldsInOrder = sleeperSchema.getAllFields();
        Set<String> rowKeyColumnNameSet = ImmutableSet.copyOf(sleeperSchema.getRowKeyFieldNames());
        Set<String> sortKeyColumnNameSet = ImmutableSet.copyOf(sleeperSchema.getSortKeyFieldNames());
        Set<String> valueColumnNameSet = ImmutableSet.copyOf(sleeperSchema.getValueFieldNames());
        List<SleeperColumnHandle> sleeperColumnHandleList = allFieldsInOrder.stream()
                .map(field -> new SleeperColumnHandle(field.getName(),
                        SleeperTypeConversionUtils.convertSleeperTypeToTrinoType(field.getType()),
                        convertColumnNameToCategory(
                                field.getName(),
                                rowKeyColumnNameSet,
                                sortKeyColumnNameSet,
                                valueColumnNameSet)))
                .collect(ImmutableList.toImmutableList());
        return new SleeperTableHandle(
                new SchemaTableName(DEFAULT_TRINO_SCHEMA_NAME, tableName),
                sleeperColumnHandleList);
    }
}
