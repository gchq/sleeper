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
import com.google.gson.Gson;
import org.apache.arrow.vector.complex.reader.FieldReader;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import sleeper.core.partition.Partition;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A handler that just passes on the default information without enhancing it. Instead of grouping the data by leaf
 * partition, this handler will find all distinct files from all the partitions and pass each one to a separate Record
 * handler - thereby achieving a higher degree of parallelism of one handler per file.
 */
public class SimpleMetadataHandler extends SleeperMetadataHandler {

    public SimpleMetadataHandler() {
        super();
    }

    public SimpleMetadataHandler(
            S3Client s3Client, DynamoDbClient dynamoDBClient, String configBucket,
            EncryptionKeyFactory encryptionKeyFactory, SecretsManagerClient secretsManager,
            AthenaClient athena, String spillBucket, String spillPrefix) {
        super(s3Client, dynamoDBClient, configBucket, encryptionKeyFactory, secretsManager, athena, spillBucket, spillPrefix);
    }

    @Override
    protected void addExtraSchemaEnhancements(SchemaBuilder partitionSchemaBuilder, GetTableLayoutRequest request) {
        // no op
    }

    @Override
    protected void writeExtraPartitionDataToBlock(Partition partition, Block block, int rowNum) {
        // no op
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request) {
        Block partitions = request.getPartitions();
        Set<String> files = new HashSet<>();
        FieldReader filesReader = partitions.getFieldReader(RELEVANT_FILES_FIELD);
        Gson gson = new Gson();
        for (int i = 0; i < partitions.getRowCount(); i++) {
            filesReader.setPosition(i);
            List<String> list = gson.fromJson(filesReader.readText().toString(), List.class);
            files.addAll(list);
        }

        Set<Split> splits = files.stream()
                .map(file -> Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
                        .add(RELEVANT_FILES_FIELD, file).build())
                .collect(Collectors.toSet());

        return new GetSplitsResponse(request.getCatalogName(), splits);
    }
}
