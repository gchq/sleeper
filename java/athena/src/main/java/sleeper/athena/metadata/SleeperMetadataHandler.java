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
package sleeper.athena.metadata;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.AllOrNoneValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.Ranges;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.row.KeyComparator;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatus;
import sleeper.statestore.StateStoreFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.ID;

/**
 * Deals with requests about the layout of a Sleeper instance. It provides information about the tables and schemas of
 * those tables. When a query is run, Athena will first make a request to this handler to find out what partitions it
 * needs to query. These are split and forwarded to the record handler used to retrieve the data.
 */
public abstract class SleeperMetadataHandler extends MetadataHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(SleeperMetadataHandler.class);

    public static final String SOURCE_TYPE = "Sleeper";
    public static final String RELEVANT_FILES_FIELD = "_SleeperRelevantFiles";
    private final InstanceProperties instanceProperties;
    private final TableIndex tableIndex;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;

    public SleeperMetadataHandler() {
        this(S3Client.create(), DynamoDbClient.create(), System.getenv(CONFIG_BUCKET.toEnvironmentVariable()));
    }

    public SleeperMetadataHandler(S3Client s3Client, DynamoDbClient dynamoClient, String configBucket) {
        super(SOURCE_TYPE, System.getenv());
        this.instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucket);
        this.tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
        this.tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
        this.stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient);
    }

    public SleeperMetadataHandler(
            S3Client s3Client, DynamoDbClient dynamoClient, String configBucket,
            EncryptionKeyFactory encryptionKeyFactory, SecretsManagerClient secretsManager,
            AthenaClient athena, String spillBucket, String spillPrefix) {
        super(encryptionKeyFactory, secretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix, System.getenv());
        this.instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucket);
        this.tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
        this.tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
        this.stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient);
    }

    /**
     * Used to get the set of schemas (aka databases) that this source contains. Using the tableLister to list the
     * tables in this instance.
     *
     * @param  blockAllocator     Tool for creating and managing Apache Arrow Blocks.
     * @param  listSchemasRequest Provides details on who made the request and which Athena catalog they are querying.
     * @return                    A ListSchemasResponse which primarily contains a Set of schema names and a catalog
     *                            name corresponding the Athena catalog that was queried.
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest listSchemasRequest) {
        LOGGER.info("Received Schema Request: {}", listSchemasRequest);
        return new ListSchemasResponse(listSchemasRequest.getCatalogName(), Sets.newHashSet(instanceProperties.get(ID)));
    }

    /**
     * Used to get a paginated list of tables that this source contains. In Sleeper the Schema is coupled to the table
     * so we can just use the schema name in the request.
     *
     * @param    blockAllocator    Tool for creating and managing Apache Arrow Blocks.
     * @param    listTablesRequest Provides details on who made the request and which Athena catalog and database they
     *                             are querying.
     * @return                     A ListTablesResponse which primarily contains a List enumerating the TableNames in
     *                             this catalog, database tuple. It also contains the catalog name corresponding the
     *                             Athena catalog that was queried.
     * @implNote                   A complete (un-paginated) list of tables should be returned if the request's pageSize
     *                             is set to ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE.
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest listTablesRequest) {
        LOGGER.info("Received List Tables Request: {}", listTablesRequest);
        String nextToken = null;
        int pageSize = listTablesRequest.getPageSize();
        String schemaName = listTablesRequest.getSchemaName();

        List<TableName> tables = tableIndex.streamAllTables()
                .map(TableStatus::getTableName)
                .sorted()
                .map(t -> new TableName(schemaName, t))
                .collect(Collectors.toList());

        // Check if request has unlimited page size. If not, then list of tables will be paginated.
        if (pageSize != UNLIMITED_PAGE_SIZE_VALUE) {
            // Get the index of the starting table for this page (if null, then this is the first page).
            String nextTokenString = listTablesRequest.getNextToken();
            int startToken = nextTokenString == null ? 0 : Integer.parseInt(nextTokenString);
            // Include all tables if the startToken is null, or only the tables whose names are equal to
            // or higher in value than startToken. Limit the number of tables in the list to the pageSize + 1 (the
            // nextToken).
            List<TableName> paginatedTables = tables.stream()
                    .skip(startToken)
                    .limit(pageSize + 1)
                    .collect(Collectors.toList());

            if (paginatedTables.size() > pageSize) {
                // Paginated list contains full page of results + nextToken.
                // nextToken is the index of the last element in the paginated list.
                nextToken = String.valueOf(pageSize);
                // nextToken is removed to include only the paginated results.
                tables = paginatedTables.subList(0, pageSize);
            } else {
                // Paginated list contains all remaining tables - end of the pagination.
                tables = paginatedTables;
            }
        }
        return new ListTablesResponse(listTablesRequest.getCatalogName(), tables, nextToken);
    }

    /**
     * Used to get definition (field names, types, descriptions, etc...) of a Table. Schema and partition information
     * is derived from the Sleeper Schema for the table requested. Even though data in Sleeper is bucketed, we don't
     * return the row keys as partitioned keys. This is because if we do this, Athena will validate the partition
     * keys when we return a result from {@code this.getPartitions()}. This usually results in Athena throwing away
     * partitions that we actually want to read.
     *
     * @param  blockAllocator  Tool for creating and managing Apache Arrow Blocks.
     * @param  getTableRequest Provides details on who made the request and which Athena catalog, database, and table
     *                         they are querying.
     * @return                 A GetTableResponse which primarily contains:
     *                         1. An Apache Arrow Schema object describing the table's columns, types, and descriptions.
     *                         2. A Set of partition column names (or empty if the table isn't partitioned).
     *                         3. A TableName object confirming the schema and table name the response is for.
     *                         4. A catalog name corresponding the Athena catalog that was queried.
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest) {
        LOGGER.info("Received Get Table Request: {}", getTableRequest);
        String tableName = getTableRequest.getTableName().getTableName();
        TableProperties tableProperties = tablePropertiesProvider.getByName(tableName);
        Schema schema = tableProperties.getSchema();
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = toArrowSchema(schema);

        return new GetTableResponse(getTableRequest.getCatalogName(), getTableRequest.getTableName(),
                arrowSchema,
                Sets.newHashSet());
    }

    /**
     * Add an extra column which will be available to the GetSplits method but not to Athena. This column is used
     * for saying which Sleeper partitions are relevant to the query
     *
     * @param partitionSchemaBuilder the builder
     * @param request                the request
     */
    @Override
    public void enhancePartitionSchema(SchemaBuilder partitionSchemaBuilder, GetTableLayoutRequest request) {
        LOGGER.debug("Enhancing schema with fields for schema description");
        partitionSchemaBuilder.addStringField(RELEVANT_FILES_FIELD);
        addExtraSchemaEnhancements(partitionSchemaBuilder, request);
    }

    /**
     * In addition to the _SleeperRelevantFiles field, add other fields that you may want to add. This will depend on
     * the implementation of {@code writeExtraPartitionDataToBlock()}
     *
     * @param partitionSchemaBuilder the schema builder
     * @param request                the request
     */
    protected abstract void addExtraSchemaEnhancements(SchemaBuilder partitionSchemaBuilder, GetTableLayoutRequest request);

    /**
     * Finds partitions that can satisfy the predicate. It iterates through the partitions, eliminating those which
     * don't contain data that satisfy the key requirements. At the end of processing it returns the relevant partitions
     * for this query.
     *
     * @param blockWriter           Used to write rows (partitions) into the Apache Arrow response.
     * @param getTableLayoutRequest Provides details of the catalog, database, and table being queried as well as any
     *                              filter predicate.
     * @param queryStatusChecker    A QueryStatusChecker that you can use to stop doing work for a query that has
     *                              already terminated.
     * @note                        Partitions are partially opaque to Amazon Athena in that it only understands your
     *                              partition columns and how to filter out partitions that do not meet the query's
     *                              constraints. Any additional columns you add to the partition data are ignored by
     *                              Athena but passed on to calls on GetSplits. Also note that the BlockWriter handlers
     *                              automatically constraining and filtering out values that don't satisfy the query's
     *                              predicate. This is how we we accomplish partition pruning. You can optionally
     *                              retrieve a ConstraintEvaluator from BlockWriter if you have your own need to apply
     *                              filtering in Lambda. Otherwise you can get the actual predicate from the request
     *                              object for pushing down into the source you are querying.
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest getTableLayoutRequest, QueryStatusChecker queryStatusChecker) throws Exception {
        LOGGER.info("Received Partition request for query: {}", getTableLayoutRequest);
        TableProperties tableProperties = getTableProperties(getTableLayoutRequest.getTableName().getTableName());
        Schema schema = tableProperties.getSchema();
        StateStore stateStore = getStateStore(tableProperties);

        List<Partition> allPartitions = stateStore.getAllPartitions();
        Map<String, List<String>> partitionToReferencedFiles = stateStore.getPartitionToReferencedFilesMap();
        PartitionTree partitionTree = new PartitionTree(allPartitions);
        // Filtering existing list to avoid expensive call to statestore
        List<Partition> leafPartitions = allPartitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toList());
        Map<String, ValueSet> predicates = getTableLayoutRequest.getConstraints().getSummary();
        List<Field> rowKeyFields = schema.getRowKeyFields();

        leafPartitions.forEach(partition -> {
            LOGGER.debug("Checking partition {} if it contains relevant files", partition.getId());
            // First Check the partition meets the constraints
            if (isValid(partition, rowKeyFields, predicates)) {
                LOGGER.debug("Partition {} contained relevant files", partition.getId());
                List<String> relevantFilesForLeafPartition = getRelevantFilesForLeafPartition(partition, partitionTree, partitionToReferencedFiles);
                if (relevantFilesForLeafPartition.isEmpty()) {
                    return;
                }

                Gson gson = new Gson();
                blockWriter.writeRows((block, rowNum) -> {
                    block.setValue(RELEVANT_FILES_FIELD, rowNum, gson.toJson(relevantFilesForLeafPartition));
                    writeExtraPartitionDataToBlock(partition, block, rowNum);
                    return 1;
                });
            } else {
                LOGGER.debug("Partition {} contained no relevant files", partition.getId());
            }
        });
    }

    /**
     * Allows per implementation customisation to the GetPartitions. Implementations may want to add extra information
     * in addition to the relevant files to the partition block. Implementations should note that any additions here
     * need to be reflected in the {@code addExtraSchemaEnhancements()} method.
     * <p>
     * By the time this method is called, the partition has already been validated and deemed necessary to query.
     *
     * @param partition the partition being queried
     * @param block     the block of data to write to
     * @param rowNum    the row number to write to
     */
    protected abstract void writeExtraPartitionDataToBlock(Partition partition, Block block, int rowNum);

    /**
     * Iterates through the row keys and tests and tests the ValueSets against the min and max provided. If all the
     * row keys pass the test, it returns true, otherwise it returns false.
     *
     * @param  partition    The partition being checked.
     * @param  rowKeyFields The Schema row key fields
     * @param  valueSets    The ValueSets to test
     * @return              true if valid, false if not
     */
    private boolean isValid(Partition partition, List<Field> rowKeyFields, Map<String, ValueSet> valueSets) {
        // Iterate through the dimensions of the key
        for (Field field : rowKeyFields) {
            sleeper.core.range.Range range = partition.getRegion().getRange(field.getName());
            PrimitiveType type = (PrimitiveType) field.getType();
            ValueSet keyPredicate = valueSets.getOrDefault(field.getName(),
                    new AllOrNoneValueSet(toArrowType(type), true, true));
            LOGGER.debug("Predicate for {} was {}", field.getName(), keyPredicate);
            Object min = range.getMin();
            Object max = range.getMax();
            // An Equatable set means one that has one or many values a user is looking for (or not looking for)
            if (keyPredicate instanceof EquatableValueSet) {
                LOGGER.debug("Predicate was EquatableValueSet");
                EquatableValueSet valueSet = (EquatableValueSet) keyPredicate;
                boolean match = partitionMatchesExactValues(type, min, max, valueSet);
                if (!match) {
                    return false;
                }
            } else if (keyPredicate.isAll()) {
                // This is the default value and could in theory be asked for by the user. It indicates that all values
                // should satisfy the predicate.
            } else if (keyPredicate.isNone()) {
                // This would be a bit odd as a user has asked for something which cannot happen. However as the API
                // seems to allow it, we should handle it without throwing an Exception.
                LOGGER.warn("Received query which was always going to return nothing");
                return false;
            } else if (keyPredicate instanceof SortedRangeSet) {
                LOGGER.debug("Key predicate should be a range");
                boolean matchedRange = partitionMatchesRange(type, keyPredicate, min, max);
                if (!matchedRange) {
                    return false;
                }
            } else {
                throw new RuntimeException("Unhandled predicate type: " + keyPredicate);
            }
        }

        return true;
    }

    /**
     * Gets files from the leaf partition all the way up to the root of the tree.
     *
     * @param  leafPartition              The leaf partition which the files may relate to
     * @param  partitionTree              A tree of all the partitions
     * @param  partitionToReferencedFiles A dictionary of partitions to their referenced files
     * @return                            All the files that relate (or could relate to) a leaf partition
     */
    private List<String> getRelevantFilesForLeafPartition(Partition leafPartition, PartitionTree partitionTree,
            Map<String, List<String>> partitionToReferencedFiles) {
        List<Partition> relevantPartitions = partitionTree.getAllAncestors(leafPartition.getId());
        relevantPartitions.add(leafPartition);

        return relevantPartitions.stream()
                .map(Partition::getId)
                .map(partitionToReferencedFiles::get)
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .distinct()
                .collect(Collectors.toList());
    }

    /**
     * Returns the TableProperties associated with a table name.
     *
     * @param  tableName the name of the table
     * @return           the table properties
     */
    protected TableProperties getTableProperties(String tableName) {
        return tablePropertiesProvider.getByName(tableName);
    }

    /**
     * The StateStore associated with a list of table properties.
     *
     * @param  tableProperties the table properties
     * @return                 a statestore
     */
    protected StateStore getStateStore(TableProperties tableProperties) {
        return stateStoreProvider.getStateStore(tableProperties);
    }

    /**
     * Converts a primitive type in Sleeper to an Arrow type.
     *
     * @param  type the primitive type
     * @return      The equivalent Arrow type
     */
    protected ArrowType toArrowType(PrimitiveType type) {
        if (type instanceof StringType) {
            return Types.MinorType.VARCHAR.getType();
        } else if (type instanceof IntType) {
            return Types.MinorType.INT.getType();
        } else if (type instanceof LongType) {
            return Types.MinorType.BIGINT.getType();
        } else if (type instanceof ByteArrayType) {
            return new ArrowType.Binary();
        } else {
            throw new RuntimeException("Unexpected primitive type: " + type);
        }
    }

    private boolean partitionMatchesRange(PrimitiveType type, ValueSet keyPredicate, Object min, Object max) {
        Ranges ranges = keyPredicate.getRanges();
        Range partitionRange;

        if (max == null) {
            partitionRange = Range.greaterThanOrEqual(new BlockAllocatorImpl(), toArrowType(type), min);
        } else {
            partitionRange = Range.range(new BlockAllocatorImpl(), toArrowType(type), min, true, max,
                    false);
        }
        boolean matchedRange = false;
        for (Range orderedRange : ranges.getOrderedRanges()) {
            if (partitionRange.overlaps(orderedRange)) {
                matchedRange = true;
                break;
            }
        }
        return matchedRange;
    }

    private boolean partitionMatchesExactValues(PrimitiveType type, Object min, Object max, EquatableValueSet valueSet) {
        boolean isAllowList = valueSet.isWhiteList();
        /*
         * Filtering partitions based on a denylist is dubious because the result is different depending on whether
         * you're dealing with discrete or continuous data. As in practice, Athena seems to convert everything to a
         * range anyway right now, I'll just allow it.
         */
        if (!isAllowList) {
            LOGGER.warn("User is using DenyList to filter key. This will not result in partition pruning from the" +
                    " query.");
            return true;
        }
        KeyComparator keyComparator = new KeyComparator(type);
        int rowCount = valueSet.getValues().getRowCount();
        boolean match = false;
        for (int j = 0; j < rowCount; j++) {
            Object value = valueSet.getValue(j);
            // If the minimum in the partition is less than or equal to the value and the maximum is more than
            // the value, the partition matches.
            if (keyComparator.compare(Key.create(min), Key.create(value)) <= 0 &&
                    keyComparator.compare(Key.create(value), Key.create(max)) < 0) {
                // This partition matches the value. If the value is being asked for, the match is positive
                match = true;
                break;
            }
        }
        return match;
    }

    private org.apache.arrow.vector.types.pojo.Schema toArrowSchema(Schema schema) {
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schema.getRowKeyFields().forEach(field -> addPrimitiveType(schemaBuilder, field));
        schema.getSortKeyFields().forEach(field -> addPrimitiveType(schemaBuilder, field));
        schema.getValueFields().forEach(field -> addType(schemaBuilder, field));

        return schemaBuilder.build();
    }

    private void addType(SchemaBuilder schemaBuilder, Field field) {
        if (!addPrimitiveType(schemaBuilder, field)) {
            addComplexType(schemaBuilder, field);
        }
    }

    private void addComplexType(SchemaBuilder schemaBuilder, Field field) {
        Type fieldType = field.getType();
        if (fieldType instanceof ListType) {
            ArrowType elementType = toArrowType(((ListType) fieldType).getElementType());
            schemaBuilder.addListField(field.getName(), elementType);
        }
        // Add support for Map types
    }

    private boolean addPrimitiveType(SchemaBuilder schemaBuilder, sleeper.core.schema.Field field) {
        Type fieldType = field.getType();
        if (fieldType instanceof StringType) {
            schemaBuilder.addStringField(field.getName());
        } else if (fieldType instanceof IntType) {
            schemaBuilder.addIntField(field.getName());
        } else if (fieldType instanceof LongType) {
            schemaBuilder.addBigIntField(field.getName());
        } else if (fieldType instanceof ByteArrayType) {
            schemaBuilder.addField(field.getName(), new ArrowType.Binary());
        } else {
            return false;
        }
        return true;
    }
}
