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
package sleeper.statestore.lambda.transaction;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import sleeper.compaction.trackerv2.job.CompactionJobTrackerFactory;
import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.statestore.transactionlog.state.StateListenerBeforeApply;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.ingest.trackerv2.job.IngestJobTrackerFactory;
import sleeper.statestorev2.StateStoreFactory;

import java.util.stream.Stream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TRACKER_ASYNC_COMMIT_UPDATES_ENABLED;

/**
 * A lambda that follows the transaction log of a Sleeper state store.
 */
public class TransactionLogFollowerLambda implements RequestHandler<DynamodbEvent, StreamsEventResponse> {
    public static final Logger LOGGER = LoggerFactory.getLogger(TransactionLogFollowerLambda.class);

    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final DynamoDBStreamTransactionLogEntryMapper mapper;
    private final StateStoreProvider stateStoreProvider;
    private final CompactionJobTracker compactionJobTracker;
    private final IngestJobTracker ingestJobTracker;

    public TransactionLogFollowerLambda() {
        String s3Bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        S3Client s3Client = S3Client.create();
        DynamoDbClient dynamoClient = DynamoDbClient.create();
        S3TransferManager s3TransferManager = S3TransferManager.builder().s3Client(S3AsyncClient.create()).build();
        instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, s3Bucket);
        tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
        stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient, s3TransferManager);
        compactionJobTracker = CompactionJobTrackerFactory.getTracker(dynamoClient, instanceProperties);
        ingestJobTracker = IngestJobTrackerFactory.getTracker(dynamoClient, instanceProperties);
        mapper = new DynamoDBStreamTransactionLogEntryMapper(TransactionSerDeProvider.from(tablePropertiesProvider));
    }

    // Used only for testing
    public TransactionLogFollowerLambda(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            DynamoDBStreamTransactionLogEntryMapper mapper, StateStoreProvider stateStoreProvider,
            CompactionJobTracker compactionJobTracker, IngestJobTracker ingestJobTracker) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.mapper = mapper;
        this.stateStoreProvider = stateStoreProvider;
        this.compactionJobTracker = compactionJobTracker;
        this.ingestJobTracker = ingestJobTracker;
    }

    @Override
    public StreamsEventResponse handleRequest(DynamodbEvent event, Context context) {
        LOGGER.info("Received event with {} records", event.getRecords().size());
        return handleRecords(mapper.toTransactionLogEntries(event.getRecords()));
    }

    /**
     * Used by the lambda handler to process transaction log entries that have been mapped from the DynamoDB stream.
     *
     * @param  entries the entries
     * @return         the result of which records failed requiring a retry
     */
    public StreamsEventResponse handleRecords(Stream<TransactionLogEntryHandle> entries) {
        entries.forEach(entry -> {
            try {
                TableProperties tableProperties = tablePropertiesProvider.getById(entry.tableId());
                TransactionLogStateStore statestore = (TransactionLogStateStore) stateStoreProvider.getStateStore(tableProperties);
                statestore.applyEntryFromLog(entry.entry(), StateListenerBeforeApply.updateTrackers(tableProperties.getStatus(), ingestJobTracker, compactionJobTracker()));
            } catch (TableNotFoundException e) {
                LOGGER.warn("Found entry for Sleeper table that does not exist: {}", entry);
            } catch (RuntimeException e) {
                LOGGER.error("Failed processing entry: {}", entry, e);
            }
        });
        return new StreamsEventResponse();
    }

    private CompactionJobTracker compactionJobTracker() {
        if (instanceProperties.getBoolean(COMPACTION_TRACKER_ASYNC_COMMIT_UPDATES_ENABLED)) {
            return compactionJobTracker;
        } else {
            return CompactionJobTracker.NONE;
        }
    }

}
