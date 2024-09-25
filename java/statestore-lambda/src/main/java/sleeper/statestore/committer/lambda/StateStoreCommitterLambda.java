/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.statestore.committer.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.statestore.StateStoreProvider;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.util.LoggedDuration;
import sleeper.core.util.PollWithRetries;
import sleeper.dynamodb.tools.DynamoDBUtils;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.committer.StateStoreCommitRequestDeserialiser;
import sleeper.statestore.committer.StateStoreCommitter;
import sleeper.statestore.committer.StateStoreCommitter.RequestHandle;
import sleeper.statestore.committer.StateStoreCommitter.RetryOnThrottling;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;

/**
 * A lambda that allows for asynchronous commits to a state store.
 */
public class StateStoreCommitterLambda implements RequestHandler<SQSEvent, SQSBatchResponse> {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreCommitterLambda.class);

    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final StateStoreCommitRequestDeserialiser deserialiser;
    private final StateStoreCommitter committer;
    private final PollWithRetries throttlingRetriesConfig;

    public StateStoreCommitterLambda() {
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        String s3Bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());

        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, s3Bucket);
        Configuration hadoopConf = HadoopConfigurationProvider.getConfigurationForLambdas(instanceProperties);

        tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDBClient);
        StateStoreFactory stateStoreFactory = StateStoreFactory.forCommitterProcess(instanceProperties, s3Client, dynamoDBClient, hadoopConf);
        stateStoreProvider = new StateStoreProvider(instanceProperties, stateStoreFactory);
        deserialiser = new StateStoreCommitRequestDeserialiser(tablePropertiesProvider, key -> s3Client.getObjectAsString(instanceProperties.get(DATA_BUCKET), key));
        committer = new StateStoreCommitter(
                CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties),
                IngestJobStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties),
                tablePropertiesProvider, stateStoreProvider,
                Instant::now);
        throttlingRetriesConfig = PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(5), Duration.ofMinutes(10));
    }

    public StateStoreCommitterLambda(
            TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider,
            StateStoreCommitRequestDeserialiser deserialiser,
            StateStoreCommitter committer,
            PollWithRetries throttlingRetriesConfig) {
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.deserialiser = deserialiser;
        this.committer = committer;
        this.throttlingRetriesConfig = throttlingRetriesConfig;
    }

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        Instant startTime = Instant.now();
        LOGGER.info("Lambda started at {}", startTime);
        List<BatchItemFailure> batchItemFailures = new ArrayList<>();
        List<RequestHandle> requests = getRequestHandlesWithFailureTracking(event,
                failed -> batchItemFailures.add(new BatchItemFailure(failed.getMessageId())));
        committer.applyBatch(retryForBatch(), requests);
        Instant finishTime = Instant.now();
        LOGGER.info("Lambda finished at {} (ran for {})",
                finishTime, LoggedDuration.withFullOutput(startTime, finishTime));
        return new SQSBatchResponse(batchItemFailures);
    }

    private List<RequestHandle> getRequestHandlesWithFailureTracking(SQSEvent event, Consumer<SQSMessage> onFail) {
        return event.getRecords().stream()
                .map(message -> readRequest(message, onFail))
                .collect(toUnmodifiableList());
    }

    private RequestHandle readRequest(SQSMessage message, Consumer<SQSMessage> onFail) {
        LOGGER.debug("Found message: {}", message.getBody());
        return RequestHandle.withCallbackOnFail(
                deserialiser.fromJson(message.getBody()),
                () -> onFail.accept(message));
    }

    private RetryOnThrottling retryForBatch() {
        PollWithRetries throttlingRetries = throttlingRetriesConfig.toBuilder()
                .trackMaxRetriesAcrossInvocations()
                .build();
        return operation -> DynamoDBUtils.retryOnThrottlingException(throttlingRetries, operation);
    }
}
