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
package sleeper.splitter.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3PropertiesReloader;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequestSerDe;
import sleeper.sketches.store.S3SketchesStore;
import sleeper.sketches.store.SketchesStore;
import sleeper.splitter.core.find.SplitPartitionJobDefinition;
import sleeper.splitter.core.find.SplitPartitionJobDefinitionSerDe;
import sleeper.splitter.core.split.SplitPartition;
import sleeper.splitter.core.split.SplitPartition.SendAsyncCommit;
import sleeper.statestore.StateStoreFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;

/**
 * Triggered by an SQS event containing a partition splitting job to do.
 */
public class SplitPartitionLambda implements RequestHandler<SQSEvent, SQSBatchResponse> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SplitPartitionLambda.class);
    private final PropertiesReloader propertiesReloader;
    private final InstanceProperties instanceProperties;
    private final StateStoreProvider stateStoreProvider;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final SketchesStore sketchesStore;
    private final SqsClient sqsClient;
    private final Supplier<String> idSupplier;

    public SplitPartitionLambda() {
        this(S3Client.builder().build(), DynamoDbClient.builder().build(), SqsClient.builder().build());
    }

    private SplitPartitionLambda(S3Client s3Client, DynamoDbClient dynamoDBClient, SqsClient sqsClient) {
        this(loadInstanceProperties(s3Client), s3Client, dynamoDBClient, sqsClient);
    }

    private SplitPartitionLambda(InstanceProperties instanceProperties, S3Client s3Client, DynamoDbClient dynamoDBClient, SqsClient sqsClient) {
        this(instanceProperties, s3Client, dynamoDBClient, sqsClient, () -> UUID.randomUUID().toString());
    }

    public SplitPartitionLambda(InstanceProperties instanceProperties, S3Client s3Client, DynamoDbClient dynamoDBClient, SqsClient sqsClient,
            Supplier<String> idSupplier) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDBClient);
        this.stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoDBClient);
        this.propertiesReloader = S3PropertiesReloader.ifConfigured(s3Client, instanceProperties, tablePropertiesProvider);
        this.sketchesStore = S3SketchesStore.createReadOnly(s3Client);
        this.sqsClient = sqsClient;
        this.idSupplier = idSupplier;
    }

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        propertiesReloader.reloadIfNeeded();
        List<BatchItemFailure> batchItemFailures = new ArrayList<>();
        for (SQSEvent.SQSMessage message : event.getRecords()) {
            try {
                splitPartitionFromJson(message.getBody());
            } catch (RuntimeException e) {
                LOGGER.error("Failed partition splitting", e);
                batchItemFailures.add(new BatchItemFailure(message.getMessageId()));
            }
        }
        return new SQSBatchResponse(batchItemFailures);
    }

    public void splitPartitionFromJson(String json) {
        SplitPartitionJobDefinition job = new SplitPartitionJobDefinitionSerDe(tablePropertiesProvider)
                .fromJson(json);
        LOGGER.info("Received partition splitting job {}", job);
        TableProperties tableProperties = tablePropertiesProvider.getById(job.getTableId());
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        SplitPartition splitPartition = new SplitPartition(stateStore, tableProperties, sketchesStore, idSupplier,
                sendAsyncCommit(sqsClient, instanceProperties, tableProperties));
        splitPartition.splitPartition(job.getPartition(), job.getFileNames());
    }

    private static InstanceProperties loadInstanceProperties(S3Client s3Client) {
        String s3Bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        if (null == s3Bucket) {
            throw new RuntimeException("Couldn't get S3 bucket from environment variable");
        }
        return S3InstanceProperties.loadFromBucket(s3Client, s3Bucket);
    }

    private static SendAsyncCommit sendAsyncCommit(SqsClient sqs, InstanceProperties instanceProperties, TableProperties tableProperties) {
        StateStoreCommitRequestSerDe serDe = new StateStoreCommitRequestSerDe(tableProperties);
        return request -> sqs.sendMessage(send -> send
                .queueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                .messageBody(serDe.toJson(request))
                .messageGroupId(request.getTableId())
                .messageDeduplicationId(UUID.randomUUID().toString()));
    }
}
