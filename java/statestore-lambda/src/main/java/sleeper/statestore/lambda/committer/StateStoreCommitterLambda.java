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
package sleeper.statestore.lambda.committer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequestSerDe;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.util.LoggedDuration;
import sleeper.core.util.PollWithRetries;
import sleeper.dynamodb.tools.DynamoDBUtils;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.committer.StateStoreCommitter;
import sleeper.statestore.committer.StateStoreCommitter.RequestHandle;
import sleeper.statestore.committer.StateStoreCommitter.RetryOnThrottling;
import sleeper.statestore.transactionlog.S3TransactionBodyStore;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * A lambda that allows for asynchronous commits to a state store.
 */
public class StateStoreCommitterLambda implements RequestHandler<SQSEvent, SQSBatchResponse> {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreCommitterLambda.class);

    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final StateStoreCommitRequestSerDe serDe;
    private final StateStoreCommitter committer;
    private final PollWithRetries throttlingRetriesConfig;

    public StateStoreCommitterLambda() {
        S3Client s3Client = S3Client.create();
        DynamoDbClient dynamoDBClient = DynamoDbClient.create();
        String s3Bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());

        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, s3Bucket);

        tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDBClient);
        StateStoreFactory stateStoreFactory = StateStoreFactory.forCommitterProcess(instanceProperties, s3Client, dynamoDBClient);
        stateStoreProvider = new StateStoreProvider(instanceProperties, stateStoreFactory);
        serDe = new StateStoreCommitRequestSerDe(tablePropertiesProvider);
        committer = new StateStoreCommitter(
                tablePropertiesProvider, stateStoreProvider,
                new S3TransactionBodyStore(instanceProperties, s3Client, TransactionSerDeProvider.from(tablePropertiesProvider)));
        throttlingRetriesConfig = PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(5), Duration.ofMinutes(10));
    }

    public StateStoreCommitterLambda(
            TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider,
            StateStoreCommitRequestSerDe serDe,
            StateStoreCommitter committer,
            PollWithRetries throttlingRetriesConfig) {
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.serDe = serDe;
        this.committer = committer;
        this.throttlingRetriesConfig = throttlingRetriesConfig;
    }

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        Instant startTime = Instant.now();
        LOGGER.info("State store committer process started at {}", startTime);
        List<BatchItemFailure> batchItemFailures = new ArrayList<>();
        List<RequestHandle> requests = getRequestHandlesWithFailureTracking(event,
                failed -> batchItemFailures.add(new BatchItemFailure(failed.getMessageId())));
        committer.applyBatch(retryForBatch(), requests);
        Instant finishTime = Instant.now();
        LOGGER.info("State store committer process finished at {} (ran for {})",
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
                serDe.fromJson(message.getBody()),
                () -> onFail.accept(message));
    }

    private RetryOnThrottling retryForBatch() {
        PollWithRetries throttlingRetries = throttlingRetriesConfig.toBuilder()
                .trackMaxRetriesAcrossInvocations()
                .build();
        return operation -> DynamoDBUtils.retryOnThrottlingException(throttlingRetries, operation);
    }
}
