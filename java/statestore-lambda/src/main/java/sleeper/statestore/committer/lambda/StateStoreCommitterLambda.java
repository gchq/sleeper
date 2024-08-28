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
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.statestore.StateStoreProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.util.LoggedDuration;
import sleeper.core.util.PollWithRetries;
import sleeper.dynamodb.tools.DynamoDBUtils;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.committer.StateStoreCommitRequest;
import sleeper.statestore.committer.StateStoreCommitRequestDeserialiser;
import sleeper.statestore.committer.StateStoreCommitter;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_COMMITTER_UPDATE_ON_EVERY_BATCH;

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

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, s3Bucket);
        Configuration hadoopConf = HadoopConfigurationProvider.getConfigurationForLambdas(instanceProperties);

        tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient);
        StateStoreFactory stateStoreFactory = StateStoreFactory.forCommitterProcess(instanceProperties, s3Client, dynamoDBClient, hadoopConf);
        stateStoreProvider = new StateStoreProvider(instanceProperties, stateStoreFactory);
        deserialiser = new StateStoreCommitRequestDeserialiser(tablePropertiesProvider, key -> s3Client.getObjectAsString(instanceProperties.get(DATA_BUCKET), key));
        committer = new StateStoreCommitter(
                CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties),
                IngestJobStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties),
                stateStoreProvider.byTableId(tablePropertiesProvider),
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
        PollWithRetries throttlingRetries = throttlingRetriesConfig.toBuilder()
                .trackMaxRetriesAcrossInvocations()
                .build();
        List<Request> requests = readRequests(event);
        updateBeforeBatch(requests);
        for (int i = 0; i < requests.size(); i++) {
            Request request = requests.get(i);
            try {
                DynamoDBUtils.retryOnThrottlingException(throttlingRetries, () -> request.applyWithCommitter());
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted applying commit request", e);
                requests.subList(i, requests.size())
                        .forEach(failed -> batchItemFailures.add(new BatchItemFailure(failed.getMessageId())));
                Thread.currentThread().interrupt();
                break;
            } catch (RuntimeException e) {
                LOGGER.error("Failed commit request", e);
                batchItemFailures.add(new BatchItemFailure(request.getMessageId()));
            }
        }
        Instant finishTime = Instant.now();
        LOGGER.info("Lambda finished at {} (ran for {})",
                finishTime, LoggedDuration.withFullOutput(startTime, finishTime));
        return new SQSBatchResponse(batchItemFailures);
    }

    private List<Request> readRequests(SQSEvent event) {
        return event.getRecords().stream()
                .map(this::readRequest)
                .collect(toUnmodifiableList());
    }

    private Request readRequest(SQSMessage message) {
        LOGGER.debug("Found message: {}", message.getBody());
        return new Request(message, deserialiser.fromJson(message.getBody()));
    }

    private void updateBeforeBatch(List<Request> requests) {
        requests.stream()
                .map(Request::getTableId).distinct()
                .forEach(this::updateBeforeBatchForTable);
    }

    private void updateBeforeBatchForTable(String tableId) {
        TableProperties tableProperties = tablePropertiesProvider.getById(tableId);
        if (!tableProperties.getBoolean(STATESTORE_COMMITTER_UPDATE_ON_EVERY_BATCH)) {
            return;
        }
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        if (!(stateStore instanceof TransactionLogStateStore)) {
            return;
        }
        TransactionLogStateStore state = (TransactionLogStateStore) stateStore;
        try {
            state.updateFromLogs();
        } catch (StateStoreException e) {
            throw new RuntimeException("Failed updating state store at start of batch", e);
        }
    }

    /**
     * Holds a state store commit request linked to the SQS message it was read from.
     */
    private class Request {
        private SQSMessage message;
        private StateStoreCommitRequest request;

        private Request(SQSMessage message, StateStoreCommitRequest request) {
            this.message = message;
            this.request = request;
        }

        String getTableId() {
            return request.getTableId();
        }

        String getMessageId() {
            return message.getMessageId();
        }

        void applyWithCommitter() {
            try {
                committer.apply(request);
            } catch (StateStoreException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
