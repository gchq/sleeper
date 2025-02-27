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
package sleeper.statestore.lambda.transaction;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.tracker.job.CompactionJobTrackerFactory;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.statestore.transactionlog.state.StateListenerBeforeApply;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.ingest.tracker.job.IngestJobTrackerFactory;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreFactory;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * A lambda that follows the transaction log of a Sleeper state store.
 */
public class TransactionLogFollowerLambda implements RequestHandler<DynamodbEvent, StreamsEventResponse> {
    public static final Logger LOGGER = LoggerFactory.getLogger(TransactionLogFollowerLambda.class);

    private final DynamoDBStreamTransactionLogEntryMapper mapper;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final CompactionJobTracker compactionJobTracker;
    private final IngestJobTracker ingestJobTracker;

    public TransactionLogFollowerLambda() {
        String s3Bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.defaultClient();
        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, s3Bucket);
        tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
        Configuration config = HadoopConfigurationProvider.getConfigurationForLambdas(instanceProperties);
        stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient, config);
        compactionJobTracker = CompactionJobTrackerFactory.getTracker(dynamoClient, instanceProperties);
        ingestJobTracker = IngestJobTrackerFactory.getTracker(dynamoClient, instanceProperties);
        mapper = new DynamoDBStreamTransactionLogEntryMapper(TransactionSerDeProvider.from(tablePropertiesProvider));
    }

    @Override
    public StreamsEventResponse handleRequest(DynamodbEvent event, Context context) {
        LOGGER.debug("Received event with {} records", event.getRecords().size());
        for (DynamodbStreamRecord record : event.getRecords()) {
            TransactionLogEntryForTable entry = mapper.toTransactionLogEntry(record);
            TableProperties tableProperties = tablePropertiesProvider.getById(entry.tableId());
            TransactionLogStateStore statestore = (TransactionLogStateStore) stateStoreProvider.getStateStore(tableProperties);
            statestore.applyEntryFromLog(entry.entry(), StateListenerBeforeApply.updateTrackers(tableProperties.getStatus(), ingestJobTracker, compactionJobTracker));
        }
        return new StreamsEventResponse();
    }

}
