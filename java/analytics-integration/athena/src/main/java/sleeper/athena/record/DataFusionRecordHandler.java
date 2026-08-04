/*
 * Copyright 2022-2026 Crown Copyright
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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.google.gson.Gson;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.utils.BinaryUtils;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Region;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;
import sleeper.foreign.bridge.FFIContext;
import sleeper.foreign.datafusion.DataFusionAwsConfig;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.QueryProcessingConfig;
import sleeper.query.datafusion.DataFusionLeafPartitionRowRetriever;
import sleeper.query.datafusion.DataFusionQueryFunctions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static sleeper.athena.metadata.IteratorApplyingMetadataHandler.ROW_KEY_PREFIX_TEST;
import static sleeper.athena.metadata.SleeperMetadataHandler.RELEVANT_FILES_FIELD;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

/**
 * Reads data directly as Apache Arrow record batches from DataFusion and copies the vectors into Athena blocks.
 * Row-key predicates are pushed down as regions and other predicates as an SQL query. Any predicates which cannot
 * be pushed to an SQL query to be performed by DataFusion are applied by Athena itself.
 */
public class DataFusionRecordHandler extends SleeperRecordHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataFusionRecordHandler.class);
    // The Athena block spiller rejects a single writeRows call that generates more than this many rows.
    private static final int MAX_ROWS_PER_WRITE_CALL = 100;

    public DataFusionRecordHandler() {
        this(S3Client.create(), DynamoDbClient.create(),
                System.getenv(CONFIG_BUCKET.toEnvironmentVariable()));
    }

    public DataFusionRecordHandler(S3Client s3Client, DynamoDbClient dynamoDB, String configBucket) {
        super(s3Client, dynamoDB, configBucket);
    }

    public DataFusionRecordHandler(S3Client s3Client, DynamoDbClient dynamoDB, String configBucket, SecretsManagerClient secretsManager, AthenaClient athena) {
        super(s3Client, dynamoDB, configBucket, secretsManager, athena);
    }

    /**
     * Builds the schema that DataFusion will read with. This is the row keys plus sort keys plus the value fields the
     * query needs: those projected by Athena and those referenced by a value-field predicate (which DataFusion
     * needs in order to evaluate the pushed-down SQL).
     *
     * @param  schema         the full table schema
     * @param  recordsRequest the records request
     * @return                the schema to read with
     */
    @Override
    protected Schema createSchemaForDataRead(Schema schema, ReadRecordsRequest recordsRequest) {
        Set<String> projected = recordsRequest.getSchema().getFields().stream()
                .map(org.apache.arrow.vector.types.pojo.Field::getName)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<String> constrained = recordsRequest.getConstraints().getSummary().keySet();
        List<Field> valueFields = schema.getValueFields().stream()
                .filter(field -> projected.contains(field.getName()) || constrained.contains(field.getName()))
                .collect(Collectors.toList());
        return Schema.builder()
                .rowKeyFields(schema.getRowKeyFields())
                .sortKeyFields(schema.getSortKeyFields())
                .valueFields(valueFields)
                .build();
    }

    @Override
    protected CloseableIterator<Row> createRowIterator(ReadRecordsRequest recordsRequest, Schema schema, TableProperties tableProperties) {
        throw new UnsupportedOperationException("DataFusionRecordHandler reads Arrow batches directly and does not create a row iterator");
    }

    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker) throws Exception {
        LOGGER.info("User {} with groups {} made data read request: {}", recordsRequest.getIdentity().getArn(),
                recordsRequest.getIdentity().getIamGroups(), recordsRequest);
        TableProperties tableProperties = getTableProperties(recordsRequest.getTableName().getTableName());
        Schema tableSchema = tableProperties.getSchema();
        Schema dataReadSchema = createSchemaForDataRead(tableSchema, recordsRequest);
        Map<String, ValueSet> constraints = recordsRequest.getConstraints().getSummary();

        Split split = recordsRequest.getSplit();
        List<String> relevantFiles = new ArrayList<>(readRelevantFiles(split));
        if (relevantFiles.isEmpty()) {
            return;
        }
        List<Field> rowKeyFields = tableSchema.getRowKeyFields();
        List<FieldAsString> rowKeys = split.getProperties().entrySet().stream()
                .filter(entry -> ROW_KEY_PREFIX_TEST.test(entry.getKey()))
                .map(entry -> new FieldAsString(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
        List<Object> minRowKeys = getRowKey(rowKeys, rowKeyFields, "Min");
        List<Object> maxRowKeys = getRowKey(rowKeys, rowKeyFields, "Max");

        AthenaRegionFactory regionFactory = new AthenaRegionFactory(tableSchema);
        Region partitionRegion = regionFactory.partitionRegion(minRowKeys, maxRowKeys);
        List<Region> regions = regionFactory.queryRegions(constraints, partitionRegion);
        String sqlQuery = new DataFusionSqlFactory(tableSchema).toSql(constraints);

        LeafPartitionQuery leafPartitionQuery = LeafPartitionQuery.builder()
                .tableId(tableProperties.get(TABLE_ID))
                .queryId("athena-datafusion")
                .subQueryId("athena-datafusion")
                .leafPartitionId("athena-datafusion")
                .partitionRegion(partitionRegion)
                .regions(regions)
                .files(relevantFiles)
                .processingConfig(QueryProcessingConfig.builder()
                        .requestedValueFields(dataReadSchema.getValueFieldNames())
                        .sqlQuery(sqlQuery)
                        .build())
                .build();

        DataFusionAwsConfig awsConfig = DataFusionAwsConfig.getDefault(getInstanceProperties());
        try (RootAllocator allocator = new RootAllocator();
                FFIContext<DataFusionQueryFunctions> context = FFIContext.getFFIContext(DataFusionQueryFunctions.class)) {
            DataFusionLeafPartitionRowRetriever retriever = new DataFusionLeafPartitionRowRetriever(awsConfig, allocator, context);
            try (ArrowReader reader = retriever.getArrowReader(leafPartitionQuery, dataReadSchema, tableProperties)) {
                copyBatchesToBlocks(spiller, reader);
            }
        }
    }

    private static void copyBatchesToBlocks(BlockSpiller spiller, ArrowReader reader) throws java.io.IOException {
        VectorSchemaRoot source = reader.getVectorSchemaRoot();
        while (reader.loadNextBatch()) {
            int rowCount = source.getRowCount();
            // The spiller rejects more than MAX_ROWS_PER_WRITE_CALL rows per call (blocks could exceed the max size),
            // so write each batch in chunks. Within a chunk we copy a column at a time, resolving the source vector by
            // name and checking the type once per column rather than once per cell.
            for (int start = 0; start < rowCount; start += MAX_ROWS_PER_WRITE_CALL) {
                int sourceOffset = start;
                int chunkRows = Math.min(MAX_ROWS_PER_WRITE_CALL, rowCount - start);
                spiller.writeRows((block, rowNum) -> {
                    for (FieldVector target : block.getFieldVectors()) {
                        FieldVector from = source.getVector(target.getField().getName());
                        if (from != null) {
                            copyColumn(target, rowNum, from, sourceOffset, chunkRows);
                        }
                    }
                    return chunkRows;
                });
            }
        }
    }

    // Copies a run of a column from a DataFusion vector into an Athena block vector, writing source rows
    // [sourceOffset, sourceOffset + rowCount) into target rows [startRow, startRow + rowCount). Where the two vectors
    // share an Arrow minor type this is a direct columnar copy; otherwise it reads and sets the value.
    private static void copyColumn(FieldVector target, int startRow, FieldVector source, int sourceOffset, int rowCount) {
        if (source.getMinorType() == target.getMinorType()) {
            for (int row = 0; row < rowCount; row++) {
                target.copyFromSafe(sourceOffset + row, startRow + row, source);
            }
        } else {
            for (int row = 0; row < rowCount; row++) {
                BlockUtils.setValue(target, startRow + row, source.getObject(sourceOffset + row));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Set<String> readRelevantFiles(Split split) {
        return new HashSet<>(new Gson().fromJson(split.getProperty(RELEVANT_FILES_FIELD), List.class));
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
        // The metadata handler names the split property <prefix>-<fieldName> (e.g. _MinRowKey-mykey).
        String fieldName = key.substring(key.indexOf('-') + 1);
        int index = dimensionOfRowKeyField(rowKeyFields, fieldName);
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

    private static int dimensionOfRowKeyField(List<Field> rowKeyFields, String fieldName) {
        for (int i = 0; i < rowKeyFields.size(); i++) {
            if (rowKeyFields.get(i).getName().equals(fieldName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Row key field not found in schema: " + fieldName);
    }
}
