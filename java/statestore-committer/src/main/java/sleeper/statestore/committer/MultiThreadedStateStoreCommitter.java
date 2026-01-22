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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.FileUtils;
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
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSerDe;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.util.LoggedDuration;
import sleeper.core.util.PollWithRetries;
import sleeper.dynamodb.tools.DynamoDBUtils;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.committer.StateStoreCommitter.RequestHandle;
import sleeper.statestore.transactionlog.S3TransactionBodyStore;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_COMMITTER_EC2_MIN_FREE_HEAP_TARGET_AMOUNT;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_COMMITTER_EC2_MIN_FREE_HEAP_TARGET_PERCENTAGE;

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
    private StateStoreProvider stateStoreProvider;
    private StateStoreCommitter committer;
    private PollWithRetries throttlingRetriesConfig;
    private long heapSpaceToKeepFree;
    private LinkedList<String> processedTableOrder = new LinkedList<>();
    private Map<String, CompletableFuture<Instant>> tableFutures = new HashMap<>();

    public MultiThreadedStateStoreCommitter(S3Client s3Client, DynamoDbClient dynamoClient, SqsClient sqsClient, String configBucketName) {
        this(s3Client, dynamoClient, sqsClient, configBucketName, null);
        this.init();
    }

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

        long heapSpaceAmountToKeepFree = instanceProperties.getBytes(STATESTORE_COMMITTER_EC2_MIN_FREE_HEAP_TARGET_AMOUNT);
        long heapSpacePercToKeepFree = (Runtime.getRuntime().maxMemory() / 100) * instanceProperties.getLong(STATESTORE_COMMITTER_EC2_MIN_FREE_HEAP_TARGET_PERCENTAGE);
        heapSpaceToKeepFree = Math.max(heapSpaceAmountToKeepFree, heapSpacePercToKeepFree);

        if (heapSpaceToKeepFree > Runtime.getRuntime().maxMemory()) {
            throw new IllegalArgumentException("This state store committer has been configured to keep at least " +
                    FileUtils.byteCountToDisplaySize(heapSpaceToKeepFree) +
                    " of heap available, but the maximum allowed heap size is only " +
                    FileUtils.byteCountToDisplaySize(Runtime.getRuntime().maxMemory()) +
                    "!");
        }
        LOGGER.info("Will aim to keep {} of heap space available for use",
                FileUtils.byteCountToDisplaySize(heapSpaceToKeepFree));

        TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
        serDe = new StateStoreCommitRequestSerDe(tablePropertiesProvider);

        StateStoreFactory stateStoreFactory = StateStoreFactory.forCommitterProcess(instanceProperties, s3Client, dynamoClient);
        stateStoreProvider = StateStoreProvider.noCacheSizeLimit(stateStoreFactory);
        transactionBodyStore = new S3TransactionBodyStore(instanceProperties, s3Client, TransactionSerDeProvider.from(tablePropertiesProvider));
        committer = new StateStoreCommitter(
                tablePropertiesProvider,
                stateStoreProvider,
                transactionBodyStore);
        throttlingRetriesConfig = PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(5), Duration.ofMinutes(10));
    }

    /**
     * Apply asynchronous commits from the committer SQS queue.
     *
     * @throws Exception Error
     */
    public void run() throws Exception {
        runUntil(() -> false);
    }

    /**
     * Apply asynchronous commits from the committer SQS queue until a particular condition is satisfied.
     *
     * @param  shouldStopRunning A function that returns true when the committer should stop processing commit requests
     * @throws Exception         Error
     */
    public void runUntil(Supplier<Boolean> shouldStopRunning) throws Exception {
        Exception err = null;

        Instant startedAt = Instant.now();
        Instant lastReceivedCommitsAt = Instant.now();

        if (qUrl == null || qUrl.isEmpty()) {
            throw new IllegalArgumentException("Missing URL to State Store Committer Queue!");
        }

        try {
            while (!shouldStopRunning.get()) {
                ReceiveMessageResponse response = sqsClient.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(qUrl)
                        .maxNumberOfMessages(10)
                        .waitTimeSeconds(20)
                        // TODO: Need to deal with commits potentially taking longer than this visibility threshold
                        .visibilityTimeout(15 * 60)
                        .build());

                if (response.hasMessages()) {
                    lastReceivedCommitsAt = Instant.now();
                    if (committer == null) {
                        init();
                    }
                }

                LOGGER.info("Received {} messages from queue, have been running for {}, last received commits {} ago",
                        response.messages().size(),
                        LoggedDuration.withShortOutput(startedAt, Instant.now()),
                        LoggedDuration.withShortOutput(lastReceivedCommitsAt, Instant.now()));

                Map<String, List<StateStoreCommitRequestWithSqsReceipt>> messagesByTableId = response.messages().stream()
                        .map(message -> {
                            LOGGER.trace("Received message: {}", message);
                            StateStoreCommitRequest request = serDe.fromJson(message.body());
                            LOGGER.trace("Received request: {}", request);
                            return new StateStoreCommitRequestWithSqsReceipt(request, message.receiptHandle());
                        })
                        .collect(Collectors.groupingBy(request -> request.getCommitRequest().getTableId()));

                // Try to make sure there is going to be enough heap space available to process these commits
                ensureEnoughHeapSpaceAvailable(messagesByTableId.keySet());

                messagesByTableId.entrySet().forEach(tableMessages -> {
                    String tableId = tableMessages.getKey();
                    List<StateStoreCommitRequestWithSqsReceipt> requestsWithHandle = tableMessages.getValue();
                    LOGGER.info("Received {} requests for table: {}", requestsWithHandle.size(), tableId);

                    // Wait until processing of previous commits for this table has finished
                    if (tableFutures.containsKey(tableId)) {
                        try {
                            tableFutures.get(tableId).get();
                        } catch (Exception e) {
                            throw new IllegalStateException("Exception was thrown during the processing of the previous batch of commits for table " + tableId + ":", e);
                        }
                    }

                    // Apply the commits for each table on a separate thread
                    CompletableFuture<Instant> task = CompletableFuture.supplyAsync(() -> {
                        processMessagesForTable(tableId, requestsWithHandle);
                        return Instant.now();
                    });

                    tableFutures.put(tableId, task);
                    processedTableOrder.remove(tableId);
                    processedTableOrder.add(tableId);
                });
            }
        } catch (Exception e) {
            LOGGER.error("Caught exception, starting graceful shutdown:", e);
            err = e;
        } finally {
            long pendingTaskCount = tableFutures.values().stream().filter(task -> !task.isDone()).count();
            if (pendingTaskCount > 0) {
                LOGGER.info("Requests for {} tables are still being processed, waiting for them to finish...", pendingTaskCount);
            }
            tableFutures.values().stream().map(task -> task.join()).collect(Collectors.toList());
            LOGGER.info("All pending requests have been actioned");
            if (err != null) {
                throw err;
            }
        }
    }

    @SuppressFBWarnings("DM_GC")
    private void ensureEnoughHeapSpaceAvailable(Set<String> requiredTableIds) {
        LOGGER.debug("UsedMem: {} FreeMem: {} HeapSize: {} MaxMem: {}",
                FileUtils.byteCountToDisplaySize(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()),
                FileUtils.byteCountToDisplaySize(Runtime.getRuntime().freeMemory()),
                FileUtils.byteCountToDisplaySize(Runtime.getRuntime().totalMemory()),
                FileUtils.byteCountToDisplaySize(Runtime.getRuntime().maxMemory()));

        long availableMemory =
                // Amount of heap space not currently in use
                Runtime.getRuntime().freeMemory() +
                // How much extra space the heap is allowed to grow to use
                        Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory();

        while (availableMemory < heapSpaceToKeepFree) {
            LOGGER.info("Removing old state stores from cache as limited memory available: {}", FileUtils.byteCountToDisplaySize(availableMemory));

            // Find a state store that we can remove from the in-mem cache that we haven't used for a while
            Optional<String> tableIdToUncache = processedTableOrder.stream().filter(tableId -> {
                // Don't remove a state store from the cache that we are about to use!
                return !requiredTableIds.contains(tableId) &&
                // Only remove a state store that we have finished using
                        tableFutures.get(tableId).isDone();
            }).findFirst();

            if (tableIdToUncache.isPresent()) {
                LOGGER.info("Removing state store for table {} from cache", tableIdToUncache.get());
                stateStoreProvider.removeStateStoreFromCache(tableIdToUncache.get());
                processedTableOrder.remove(tableIdToUncache.get());
            } else {
                LOGGER.error("Couldn't find any candidate state stores to remove from memory. All must currently be in use, will wait and try again...");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            System.gc();

            availableMemory = Runtime.getRuntime().freeMemory() + Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory();
            LOGGER.info("Memory now available: {}", FileUtils.byteCountToDisplaySize(availableMemory));
        }
    }

    private boolean processMessagesForTable(String tableId, List<StateStoreCommitRequestWithSqsReceipt> requestsWithHandle) {
        Instant startedAt = Instant.now();
        LOGGER.info("Processing {} requests for table: {} ...", requestsWithHandle.size(), tableId);

        committer.applyBatch(
                operation -> DynamoDBUtils.retryOnThrottlingException(throttlingRetriesConfig, operation),
                requestsWithHandle.stream().map(StateStoreCommitRequestWithSqsReceipt::getHandle).collect(Collectors.toList()));

        Map<Boolean, List<StateStoreCommitRequestWithSqsReceipt>> requestResults = requestsWithHandle.stream().collect(Collectors.partitioningBy(StateStoreCommitRequestWithSqsReceipt::failed));
        List<StateStoreCommitRequestWithSqsReceipt> failedRequests = requestResults.get(true);
        List<StateStoreCommitRequestWithSqsReceipt> successfulRequests = requestResults.get(false);

        if (successfulRequests.size() > 0) {
            LOGGER.debug("Deleting {} requests for table {} as they have been successfully applied", successfulRequests.size(), tableId);
            DeleteMessageBatchResponse deleteResponse = sqsClient.deleteMessageBatch(DeleteMessageBatchRequest.builder()
                    .queueUrl(qUrl)
                    .entries(
                            Streams.mapWithIndex(successfulRequests.stream(), (request, index) -> DeleteMessageBatchRequestEntry.builder()
                                    .id(request.getCommitRequest().getTableId() + "-" + index)
                                    .receiptHandle(request.getSqsReceipt())
                                    .build())
                                    .collect(Collectors.toList()))
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
            }

            ChangeMessageVisibilityBatchResponse changeVisibilityResponse = sqsClient.changeMessageVisibilityBatch(ChangeMessageVisibilityBatchRequest.builder()
                    .queueUrl(qUrl)
                    .entries(
                            Streams.mapWithIndex(failedRequests.stream(), (request, index) -> ChangeMessageVisibilityBatchRequestEntry.builder()
                                    .id(request.getCommitRequest().getTableId() + "-" + index)
                                    .receiptHandle(request.getSqsReceipt())
                                    .visibilityTimeout(0)
                                    .build())
                                    .collect(Collectors.toList()))
                    .build());

            if (!changeVisibilityResponse.failed().isEmpty()) {
                LOGGER.error("Failed to change visibility of {} requests for table {} in SQS queue! (successfully change visibility of {} requests):\n{}",
                        changeVisibilityResponse.failed().size(),
                        tableId,
                        changeVisibilityResponse.successful().size(),
                        String.join("\n", changeVisibilityResponse.failed().stream()
                                .map(failedRequest -> failedRequest.toString())
                                .collect(Collectors.toList())));
            } else {
                LOGGER.debug("Successfully changed visibility of {} requests for table {} in SQS queue", changeVisibilityResponse.successful().size(), tableId);
            }
        }

        LOGGER.info("Finished processing {} messages for table {} in {} ...",
                requestsWithHandle.size(),
                tableId,
                LoggedDuration.withShortOutput(startedAt, Instant.now()));
        return true;
    }

    /**
     * TODO: Explain why this is required.
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

        private RequestHandle getHandle() {
            return RequestHandle.withCallbackOnFail(commitRequest, e -> {
                LOGGER.error("Error whilst processing state store commit request for table: {}", commitRequest.getTableId(), e);
                failed = true;
            });
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
