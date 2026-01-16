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
package sleeper.athena.record;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.google.gson.Gson;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.utils.BinaryUtils;

import sleeper.athena.FilterTranslator;
import sleeper.configuration.jars.S3UserJarsLoader;
import sleeper.core.iterator.IteratorConfig;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.iterator.IteratorFactory;
import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.query.core.rowretrieval.RowRetrievalException;
import sleeper.query.runner.rowretrieval.LeafPartitionRowRetrieverImpl;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static sleeper.athena.metadata.IteratorApplyingMetadataHandler.ROW_KEY_PREFIX_TEST;
import static sleeper.athena.metadata.SleeperMetadataHandler.RELEVANT_FILES_FIELD;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * Retrieves data using Parquet's predicate pushdown, applying compaction time iterators. Searches within a single
 * partition for data which matches the constraints of the query. To protect against queries which span multiple
 * partitions, only data in parent partitions which also fall into the constraints of the leaf partition are returned.
 * <p>
 * Compaction time iterators are also applied to the results before they are returned.
 */
public class IteratorApplyingRecordHandler extends SleeperRecordHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(IteratorApplyingRecordHandler.class);

    private final ExecutorService executorService = Executors.newFixedThreadPool(10);
    private final ObjectFactory objectFactory;

    public IteratorApplyingRecordHandler() {
        this(S3Client.create(), DynamoDbClient.create(),
                System.getenv(CONFIG_BUCKET.toEnvironmentVariable()));
    }

    public IteratorApplyingRecordHandler(S3Client s3Client, DynamoDbClient dynamoDB, String configBucket) {
        super(s3Client, dynamoDB, configBucket);
        objectFactory = createObjectFactory(s3Client);
    }

    public IteratorApplyingRecordHandler(S3Client s3Client, DynamoDbClient dynamoDB, String configBucket, SecretsManagerClient secretsManager, AthenaClient athena) {
        super(s3Client, dynamoDB, configBucket, secretsManager, athena);
        objectFactory = createObjectFactory(s3Client);
    }

    private ObjectFactory createObjectFactory(S3Client s3Client) {
        try {
            return new S3UserJarsLoader(getInstanceProperties(), s3Client, Path.of("/tmp")).buildObjectFactory();
        } catch (ObjectFactoryException e) {
            throw new RuntimeException("Failed to initialise Object Factory");
        }
    }

    /**
     * The iterator may need the records sorted and may need certain fields to be present. Without knowing this
     * information (to be added in a separate issue, there's no way of slimming down the schema). Once this is
     * done, we can limit the value fields. (The merging iterator still requires the values to be sorted).
     *
     * @param  schema         the original schema
     * @param  recordsRequest the request
     * @return                the original schema for now
     */
    @Override
    protected Schema createSchemaForDataRead(Schema schema, ReadRecordsRequest recordsRequest) {
        return schema;
    }

    @Override
    protected CloseableIterator<Row> createRowIterator(ReadRecordsRequest recordsRequest, Schema schema,
            TableProperties tableProperties) throws RowRetrievalException, IteratorCreationException {
        Split split = recordsRequest.getSplit();
        Set<String> relevantFiles = new HashSet<>(new Gson().fromJson(split.getProperty(RELEVANT_FILES_FIELD), List.class));
        List<Field> rowKeyFields = schema.getRowKeyFields();

        List<FieldAsString> rowKeys = split.getProperties().entrySet().stream()
                .filter(entry -> ROW_KEY_PREFIX_TEST.test(entry.getKey()))
                .map(entry -> new FieldAsString(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        List<Object> minRowKeys = getRowKey(rowKeys, rowKeyFields, "Min");
        List<Object> maxRowKeys = getRowKey(rowKeys, rowKeyFields, "Max");

        return createIterator(relevantFiles, minRowKeys, maxRowKeys, schema, tableProperties, recordsRequest.getConstraints().getSummary());
    }

    private List<Object> getRowKey(List<FieldAsString> rowKeyStream, List<Field> rowKeyFields, String indicator) {
        List<Object> rowKey = new ArrayList<>();
        for (int i = 0; i < rowKeyFields.size(); i++) {
            rowKey.add(null);
        }

        rowKeyStream.stream()
                .filter(entry -> entry.fieldName().contains(indicator))
                .map(entry -> getFieldAtDimension(rowKeyFields, entry))
                .forEach(valueWithIndex -> rowKey.set(valueWithIndex.dimension(), valueWithIndex.value()));

        return rowKey;
    }

    private FieldAtDimension getFieldAtDimension(List<Field> rowKeyFields, FieldAsString entry) {
        String key = entry.fieldName();
        Integer index = Integer.valueOf(key.substring(key.lastIndexOf("RowKey") + 6));
        String stringValue = entry.value();
        Type type = rowKeyFields.get(index).getType();
        if (type instanceof StringType) {
            return new FieldAtDimension(index, stringValue);
        } else if (type instanceof ByteArrayType) {
            return new FieldAtDimension(index, BinaryUtils.fromBase64(stringValue));
        } else if (type instanceof IntType) {
            return new FieldAtDimension(index, Integer.parseInt(stringValue));
        } else if (type instanceof LongType) {
            return new FieldAtDimension(index, Long.parseLong(stringValue));
        } else {
            throw new RuntimeException("Unexpected Primitive type: " + type);
        }
    }

    /**
     * Creates an iterator which will read all the Parquet files relevant to the leaf partition, pushing down any
     * predicates derived from the query. It also applies any Table specific iterators that may have been configured.
     *
     * @param  relevantFiles             list of relevant partitions (the first should be the leaf partition)
     * @param  minRowKeys                the min row keys for this leaf partition
     * @param  maxRowKeys                the max row keys for this leaf partition
     * @param  schema                    the schema to use for reading the data
     * @param  tableProperties           the table properties for this table
     * @param  valueSets                 a summary of the predicates associated with this query
     * @return                           a single iterator of rows
     * @throws IteratorCreationException if something goes wrong creating the iterators
     * @throws RowRetrievalException     if something goes wrong retrieving rows
     */
    private CloseableIterator<Row> createIterator(
            Set<String> relevantFiles, List<Object> minRowKeys, List<Object> maxRowKeys,
            Schema schema, TableProperties tableProperties, Map<String, ValueSet> valueSets) throws IteratorCreationException, RowRetrievalException {
        FilterTranslator filterTranslator = new FilterTranslator(schema);
        FilterPredicate filterPredicate = FilterTranslator.and(filterTranslator.toPredicate(valueSets), createFilter(schema, minRowKeys, maxRowKeys));
        Configuration conf = getConfigurationForTable(tableProperties);

        LeafPartitionRowRetrieverImpl rowRetriever = new LeafPartitionRowRetrieverImpl(executorService, conf, tableProperties);

        CloseableIterator<Row> iterator = rowRetriever.getRows(new ArrayList<>(relevantFiles), schema, filterPredicate);

        // Apply Compaction time iterator
        return applyCompactionIterators(iterator, schema, tableProperties);

    }

    /**
     * Creates a filter to ensure rows returned from the data files fall within the scope of the leaf partition
     * that was queried.
     *
     * @param  schema     the Sleeper schema
     * @param  minRowKeys the min row keys of the leaf partition
     * @param  maxRowKeys the max row keys of the leaf partition
     * @return            a filter that ensures a row falls within the leaf partition queried
     */
    private FilterPredicate createFilter(Schema schema, List<Object> minRowKeys, List<Object> maxRowKeys) {
        List<Field> rowKeyFields = schema.getRowKeyFields();
        FilterTranslator filterTranslator = new FilterTranslator(schema);
        Map<String, ValueSet> rangeSummary = new HashMap<>();
        for (int i = 0; i < rowKeyFields.size(); i++) {
            Field field = rowKeyFields.get(i);
            Type type = field.getType();
            String name = field.getName();
            ArrowType arrowType;
            if (type instanceof IntType) {
                arrowType = Types.MinorType.INT.getType();
            } else if (type instanceof LongType) {
                arrowType = Types.MinorType.BIGINT.getType();
            } else if (type instanceof StringType) {
                arrowType = Types.MinorType.VARCHAR.getType();
            } else if (type instanceof ByteArrayType) {
                arrowType = Types.MinorType.VARBINARY.getType();
            } else {
                LOGGER.warn("Received Non standard primitive type in a row key: {}. Ignoring for now", type);
                continue;
            }

            Object max = maxRowKeys.get(i);

            SortedRangeSet predicate = max == null ? SortedRangeSet.of(Range.greaterThanOrEqual(new BlockAllocatorImpl(), arrowType, minRowKeys.get(i)))
                    : SortedRangeSet.of(Range.range(new BlockAllocatorImpl(), arrowType, minRowKeys.get(i), true, max, false));

            rangeSummary.put(name, predicate);
        }

        return filterTranslator.toPredicate(rangeSummary);
    }

    /**
     * Applies an iterator configured for this table. This iterator will run before it passes to Athena.
     *
     * @param  mergingIterator           an iterator encompassing all the Parquet iterators
     * @param  schema                    the schema to use for reading the data
     * @param  tableProperties           the table properties for the table being queried
     * @return                           a combined iterator
     * @throws IteratorCreationException if the iterator can't be instantiated
     */

    private CloseableIterator<Row> applyCompactionIterators(CloseableIterator<Row> mergingIterator, Schema schema, TableProperties tableProperties) throws IteratorCreationException {
        return new IteratorFactory(objectFactory)
                .getIterator(IteratorConfig.from(tableProperties), schema)
                .applyTransform(mergingIterator);
    }

}
