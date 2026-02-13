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
package sleeper.statestore.committer;

import com.google.common.collect.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSerDe;
import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.util.LoggedDuration;
import sleeper.core.util.PollWithRetries;
import sleeper.dynamodb.tools.DynamoDBUtils;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.committer.StateStoreCommitter.RetryOnThrottling;
import sleeper.statestore.transactionlog.S3TransactionBodyStore;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;

/**
 * Applies asynchronous commits to state stores in a manner optimised for multi core execution environments.
 */
public class MultiThreadedStateStoreCommitter {
    public static final Logger LOGGER = LoggerFactory.getLogger(MultiThreadedStateStoreCommitter.class);

    private final S3Client s3Client;
    private final DynamoDbClient dynamoClient;
    private final SqsClient sqsClient;
    private final String configBucketName;
    private String qUrl;
    private StateStoreCommitRequestSerDe serDe;
    private S3TransactionBodyStore transactionBodyStore;
    private TablePropertiesProvider tablePropertiesProvider;
    private StateStoreProvider stateStoreProvider;
    private RetryOnThrottling retryOnThrottling;
    private Map<String, CompletableFuture<Instant>> tableFutures = new HashMap<>();

    public MultiThreadedStateStoreCommitter(S3Client s3Client, DynamoDbClient dynamoClient, SqsClient sqsClient, String configBucketName, String qUrl) {
        this.s3Client = s3Client;
        this.dynamoClient = dynamoClient;
        this.sqsClient = sqsClient;
        this.configBucketName = configBucketName;
        this.qUrl = qUrl;
    }

    private void init() {
        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucketName);

        // If the URL to the SQS queue hasn't already been provided, retrieve it from instance properties
        if (qUrl == null || qUrl.isEmpty()) {
            qUrl = instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL);
        }

        tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
        serDe = new StateStoreCommitRequestSerDe(tablePropertiesProvider);

        StateStoreFactory stateStoreFactory = StateStoreFactory.forCommitterProcess(instanceProperties, s3Client, dynamoClient);
        stateStoreProvider = StateStoreProvider.memoryLimitOnly(instanceProperties, stateStoreFactory);
        transactionBodyStore = new S3TransactionBodyStore(instanceProperties, s3Client, TransactionSerDeProvider.from(tablePropertiesProvider));

        PollWithRetries throttlingRetriesConfig = PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(5), Duration.ofMinutes(10));
        retryOnThrottling = operation -> DynamoDBUtils.retryOnThrottlingException(throttlingRetriesConfig, operation);
    }

    /**
     * Apply asynchronous commits from the committer SQS queue.
     */
    public void run() {
        runUntil(20, response -> false);
    }

    /**
     * Apply asynchronous commits from the committer SQS queue until a particular condition is satisfied.
     *
     * @param waitTimeSeconds   the amount of time to wait for a message from the queue
     * @param shouldStopRunning a function that returns true when the committer should stop processing commit requests
     */
    public void runUntil(int waitTimeSeconds, Function<ReceiveMessageResponse, Boolean> shouldStopRunning) {

        Instant startedAt = Instant.now();
        Instant lastReceivedCommitsAt = Instant.now();

        if (qUrl == null || qUrl.isEmpty()) {
            throw new IllegalArgumentException("Missing URL to State Store Committer Queue!");
        }

        try {
            ReceiveMessageResponse response = null;
            while (response == null || !shouldStopRunning.apply(response)) {
                response = sqsClient.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(qUrl)
                        .maxNumberOfMessages(10)
                        .waitTimeSeconds(waitTimeSeconds)
                        // TODO: Need to deal with commits potentially taking longer than this visibility threshold
                        .visibilityTimeout(15 * 60)
                        .build());

                // Only initialise after we receive the first messages, because the instance properties may not have
                // been written to the config bucket yet when we first start up.
                if (response.hasMessages()) {
                    lastReceivedCommitsAt = Instant.now();
                    if (stateStoreProvider == null) {
                        init();
                    }
                }

                LOGGER.info("Received {} messages from queue, have been running for {}, last received commits {} ago",
                        response.messages().size(),
                        LoggedDuration.withShortOutput(startedAt, Instant.now()),
                        LoggedDuration.withShortOutput(lastReceivedCommitsAt, Instant.now()));

                Map<String, List<StateStoreCommitRequestWithSqsReceipt>> commitRequestsByTableId = response.messages().stream()
                        .map(message -> {
                            LOGGER.trace("Received message: {}", message);
                            StateStoreCommitRequest request = serDe.fromJson(message.body());
                            LOGGER.trace("Received request: {}", request);
                            return new StateStoreCommitRequestWithSqsReceipt(request, message.receiptHandle());
                        })
                        .collect(Collectors.groupingBy(request -> request.getCommitRequest().getTableId()));

                // Try to make sure there is going to be enough heap space available to process these commits
                Set<String> stateStoresToKeepInCache = tableFutures.entrySet().stream()
                        .filter(tableFuture -> !tableFuture.getValue().isDone())
                        .map(tableFuture -> tableFuture.getKey())
                        .collect(toSet());
                stateStoresToKeepInCache.addAll(commitRequestsByTableId.keySet());
                ensureEnoughHeapSpaceAvailable(stateStoresToKeepInCache);

                commitRequestsByTableId.entrySet().forEach(tableCommitRequests -> {
                    String tableId = tableCommitRequests.getKey();
                    List<StateStoreCommitRequestWithSqsReceipt> requests = tableCommitRequests.getValue();
                    LOGGER.info("Received {} requests for table: {}", requests.size(), tableId);

                    // Wait until processing of previous commits for this table has finished
                    if (tableFutures.containsKey(tableId)) {
                        try {
                            tableFutures.get(tableId).get();
                        } catch (Exception e) {
                            throw new IllegalStateException("Exception was thrown during the processing of the previous batch of commits for table " + tableId + ":", e);
                        }
                    }

                    TableProperties tableProperties = tablePropertiesProvider.getById(tableId);
                    StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);

                    // Apply the commits for each table on a separate thread
                    CompletableFuture<Instant> task = CompletableFuture.supplyAsync(() -> {
                        processCommitRequestsForTable(tableId, stateStore, requests);
                        return Instant.now();
                    });

                    tableFutures.put(tableId, task);
                });
            }
        } catch (RuntimeException e) {
            LOGGER.error("Caught exception, starting graceful shutdown:", e);
            throw e;
        } finally {
            long pendingTaskCount = tableFutures.values().stream().filter(task -> !task.isDone()).count();
            if (pendingTaskCount > 0) {
                LOGGER.info("Requests for {} tables are still being processed, waiting for them to finish...", pendingTaskCount);
            }
            tableFutures.values().stream().forEach(CompletableFuture::join);
            LOGGER.info("All pending requests have been actioned");
        }
    }

    private void ensureEnoughHeapSpaceAvailable(Set<String> tableIds) {
        if (stateStoreProvider == null) {
            return;
        }
        while (!stateStoreProvider.ensureEnoughHeapSpaceAvailable(tableIds)) {
            LOGGER.error("Couldn't find any candidate state stores to remove from memory. All must currently be in use, will wait and try again...");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    private void processCommitRequestsForTable(String tableId, StateStore stateStore, List<StateStoreCommitRequestWithSqsReceipt> requests) {
        Instant startedAt = Instant.now();
        LOGGER.info("State store committer process started at {}", startedAt);
        LOGGER.info("Processing {} requests for table: {} ...", requests.size(), tableId);
        applyBatchOfCommits(retryOnThrottling, stateStore, requests);
        reportCommitOutcomesToSqs(tableId, requests);
        Instant finishTime = Instant.now();
        LOGGER.info("State store committer process finished at {} (ran for {})",
                finishTime, LoggedDuration.withFullOutput(startedAt, finishTime));
        LOGGER.info("Finished applying batch of {} commit requests for table {} in {}",
                requests.size(),
                tableId,
                LoggedDuration.withShortOutput(startedAt, Instant.now()));
    }

    /**
     * Applies a batch of state store commit requests.
     *
     * @param retryOnThrottling function to apply retries due to DynamoDB API throttling
     * @param stateStore        state store of the Sleeper table that the commit requests should be applied to
     * @param requests          the commit requests
     */
    private void applyBatchOfCommits(RetryOnThrottling retryOnThrottling, StateStore stateStore, List<StateStoreCommitRequestWithSqsReceipt> requests) {
        for (int i = 0; i < requests.size(); i++) {
            StateStoreCommitRequestWithSqsReceipt request = requests.get(i);
            try {
                retryOnThrottling.doWithRetries(() -> applyCommit(stateStore, request));
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted applying commit request", e);
                requests.subList(i, requests.size())
                        .forEach(failed -> failed.setFailed(e));
                return;
            } catch (RuntimeException e) {
                LOGGER.error("Failed commit request", e);
                request.setFailed(e);
            }
        }
    }

    /**
     * Applies a state store commit request.
     *
     * @param stateStore state store of the Sleeper table that the commit request should be applied to
     * @param request    the commit request
     */
    private void applyCommit(StateStore stateStore, StateStoreCommitRequestWithSqsReceipt request) throws StateStoreException {
        stateStore.addTransaction(
                AddTransactionRequest.withTransaction(transactionBodyStore.getTransaction(request.getCommitRequest()))
                        .bodyKey(request.getCommitRequest().getBodyKey())
                        .build());
        LOGGER.info("Applied request to table ID {} with type {} at time {}",
                request.getCommitRequest().getTableId(), request.getCommitRequest().getTransactionType(), Instant.now());
    }

    private void reportCommitOutcomesToSqs(String tableId, List<StateStoreCommitRequestWithSqsReceipt> requests) {
        Map<Boolean, List<StateStoreCommitRequestWithSqsReceipt>> requestResults = requests.stream().collect(Collectors.partitioningBy(StateStoreCommitRequestWithSqsReceipt::failed));
        List<StateStoreCommitRequestWithSqsReceipt> failedRequests = requestResults.get(true);
        List<StateStoreCommitRequestWithSqsReceipt> successfulRequests = requestResults.get(false);

        if (successfulRequests.size() > 0) {
            LOGGER.debug("Deleting {} requests for table {} as they have been successfully applied", successfulRequests.size(), tableId);
            DeleteMessageBatchResponse deleteResponse = sqsClient.deleteMessageBatch(
                    DeleteMessageBatchRequest.builder()
                            .queueUrl(qUrl)
                            .entries(Streams.mapWithIndex(successfulRequests.stream(),
                                    (request, index) -> DeleteMessageBatchRequestEntry.builder()
                                            .id(request.getCommitRequest().getTableId() + "-" + index)
                                            .receiptHandle(request.getSqsReceipt())
                                            .build())
                                    .toList())
                            .build());

            if (!deleteResponse.failed().isEmpty()) {
                LOGGER.error("Failed to delete {} requests for table {} from SQS queue! Successfully deleted {} requests: {}",
                        deleteResponse.failed().size(),
                        tableId,
                        deleteResponse.successful().size(),
                        deleteResponse.failed());
            } else {
                LOGGER.debug("Successfully deleted {} requests for table {} from SQS queue", deleteResponse.successful().size(), tableId);
            }
        }

        if (failedRequests.size() > 0) {
            LOGGER.debug("Resetting the message visibility of {} requests for table {} so they are immediately available for reprocessing", failedRequests.size(), tableId);

            // TODO: Better retry handling
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            ChangeMessageVisibilityBatchResponse changeVisibilityResponse = sqsClient.changeMessageVisibilityBatch(
                    ChangeMessageVisibilityBatchRequest.builder()
                            .queueUrl(qUrl)
                            .entries(Streams.mapWithIndex(failedRequests.stream(),
                                    (request, index) -> ChangeMessageVisibilityBatchRequestEntry.builder()
                                            .id(request.getCommitRequest().getTableId() + "-" + index)
                                            .receiptHandle(request.getSqsReceipt())
                                            .visibilityTimeout(0)
                                            .build())
                                    .toList())
                            .build());

            if (!changeVisibilityResponse.failed().isEmpty()) {
                LOGGER.error("Failed to change visibility of {} requests for table {} in SQS queue! (successfully changed visibility of {} requests):\n{}",
                        changeVisibilityResponse.failed().size(),
                        tableId,
                        changeVisibilityResponse.successful().size(),
                        changeVisibilityResponse.failed().stream()
                                .map(failedRequest -> failedRequest.toString())
                                .collect(joining("\n")));
            } else {
                LOGGER.debug("Successfully changed visibility of {} requests for table {} in SQS queue", changeVisibilityResponse.successful().size(), tableId);
            }
        }
    }

    /**
     * Tracks whether a state store commit requests succeeded or failed, so that we can report this to SQS.
     */
    private static class StateStoreCommitRequestWithSqsReceipt {

        private StateStoreCommitRequest commitRequest;
        private String sqsReceipt;
        private boolean failed = false;

        private StateStoreCommitRequestWithSqsReceipt(StateStoreCommitRequest request, String sqsReceipt) {
            this.commitRequest = request;
            this.sqsReceipt = sqsReceipt;
        }

        private StateStoreCommitRequest getCommitRequest() {
            return commitRequest;
        }

        private void setFailed(Exception e) {
            LOGGER.error("Error whilst processing state store commit request for table: {}", commitRequest.getTableId(), e);
            failed = true;
        }

        private String getSqsReceipt() {
            return sqsReceipt;
        }

        private boolean failed() {
            return failed;
        }

    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 2) {
            throw new IllegalArgumentException("Syntax: " + MultiThreadedStateStoreCommitter.class.getSimpleName() + " <instanceId> [<stateStoreCommitQueueUrl>]");
        }

        String instanceId = args[0];
        String qUrl = args.length > 1 ? args[1] : null;

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                SqsClient sqsClient = buildAwsV2Client(SqsClient.builder())) {
            String configBucketName = InstanceProperties.getConfigBucketFromInstanceId(instanceId);
            MultiThreadedStateStoreCommitter committer = new MultiThreadedStateStoreCommitter(s3Client, dynamoClient, sqsClient, configBucketName, qUrl);
            committer.run();
        }
    }

}
