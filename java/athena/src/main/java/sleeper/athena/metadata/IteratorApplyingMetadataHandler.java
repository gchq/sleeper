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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.utils.BinaryUtils;

import sleeper.core.partition.Partition;
import sleeper.core.range.Range;
import sleeper.core.schema.Field;
import sleeper.core.schema.type.PrimitiveType;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Sends requested files in a batch grouped by partition. Allows compaction time iterators to be applied.
 * To assist with this, the row keys are written to properties in the split so the record handler doesn't have to query
 * the statestore again to find out what they are.
 */
public class IteratorApplyingMetadataHandler extends SleeperMetadataHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(IteratorApplyingMetadataHandler.class);

    public static final String MIN_ROW_KEY_PREFIX = "_MinRowKey";
    public static final String MAX_ROW_KEY_PREFIX = "_MaxRowKey";
    public static final Predicate<String> ROW_KEY_PREFIX_TEST = Pattern.compile("_M[a-z]{2}RowKey").asPredicate();

    public IteratorApplyingMetadataHandler() {
        super();
    }

    public IteratorApplyingMetadataHandler(
            S3Client s3Client, DynamoDbClient dynamoClient, String configBucket,
            EncryptionKeyFactory encryptionKeyFactory, SecretsManagerClient secretsManager,
            AthenaClient athena, String spillBucket, String spillPrefix) {
        super(s3Client, dynamoClient, configBucket, encryptionKeyFactory, secretsManager, athena, spillBucket, spillPrefix);
    }

    @Override
    protected void addExtraSchemaEnhancements(SchemaBuilder partitionSchemaBuilder, GetTableLayoutRequest request) {
        List<Field> rowKeyFields = getTableProperties(request.getTableName().getTableName()).getSchema().getRowKeyFields();
        for (Field field : rowKeyFields) {
            ArrowType arrowType = toArrowType((PrimitiveType) field.getType());
            partitionSchemaBuilder.addField(MIN_ROW_KEY_PREFIX + "-" + field.getName(), arrowType);
            partitionSchemaBuilder.addField(MAX_ROW_KEY_PREFIX + "-" + field.getName(), arrowType);
        }
    }

    @Override
    protected void writeExtraPartitionDataToBlock(Partition partition, Block block, int rowNum) {
        Collection<Range> ranges = partition.getRegion().getRangesUnordered();
        for (Range range : ranges) {
            block.setValue(MIN_ROW_KEY_PREFIX + "-" + range.getFieldName(), rowNum, range.getMin());
            block.setValue(MAX_ROW_KEY_PREFIX + "-" + range.getFieldName(), rowNum, range.getMax());
        }
    }

    /**
     * Used to create splits from partitions. The partitionId is added to the
     * split.
     *
     * @param  blockAllocator   Tool for creating and managing Apache Arrow Blocks.
     * @param  getSplitsRequest Provides details of the catalog, database, table,
     *                          and partition(s) being queried as well as any filter predicate.
     * @return                  A GetSplitsResponse which primarily contains: 1. A Set of Splits
     *                          which represent read operations Amazon Athena must perform by calling
     *                          your read function. 2. (Optional) A continuation token which allows you
     *                          to paginate the generation of splits for large queries.
     * @note                    A Split is a mostly opaque object to Amazon Athena. Amazon Athena
     *                          will use the optional SpillLocation and optional EncryptionKey for
     *                          pipelined reads but all properties you set on the Split are passed to
     *                          your read function to help you perform the read.
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest) {
        LOGGER.info("Received request for splits: {}", getSplitsRequest);
        Set<Split> splits = new HashSet<>();

        // Split based on partitions
        Block athenaPartitions = getSplitsRequest.getPartitions();

        List<FieldReader> readers = athenaPartitions.getFieldReaders().stream()
                .filter(reader -> ROW_KEY_PREFIX_TEST.test(reader.getField().getName()))
                .collect(Collectors.toList());

        readers.add(athenaPartitions.getFieldReader(RELEVANT_FILES_FIELD));

        for (int i = 0; i < athenaPartitions.getRowCount(); i++) {
            Split.Builder builder = Split.newBuilder(makeSpillLocation(getSplitsRequest), makeEncryptionKey());
            for (FieldReader reader : readers) {
                reader.setPosition(i);
                String stringValue = stringValue(reader.readObject());
                if (stringValue != null) {
                    builder.add(reader.getField().getName(), stringValue);
                }
            }
            splits.add(builder.build());
        }

        return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits);
    }

    private String stringValue(Object obj) {
        if (obj == null) {
            return null;
        } else if (obj instanceof byte[]) {
            return BinaryUtils.toBase64((byte[]) obj);
        } else {
            return obj.toString();
        }
    }
}
