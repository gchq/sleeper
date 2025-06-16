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

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.util.LoggedDuration;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.S3TransactionBodyStore;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class StateStoreCommitterThroughputIT extends LocalStackTestBase {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreCommitterThroughputIT.class);

    private final InstanceProperties instanceProperties = createInstance();

    @Test
    void shouldSendManyAddFilesRequestsWithNoJob() throws Exception {
        Stats stats10 = runAddFilesRequestsWithNoJobGetStats(10);
        // Stats stats200 = runAddFilesRequestsWithNoJobGetStats(200);
        // Stats stats1000 = runAddFilesRequestsWithNoJobGetStats(1000);
        stats10.log();
        // stats200.log();
        // stats1000.log();
    }

    private Stats runAddFilesRequestsWithNoJobGetStats(int numberOfRequests) throws Exception {
        Schema schema = createSchemaWithKey("key", new StringType());
        String tableId = createTable(schema).get(TABLE_ID);
        StateStoreCommitter committer = committer();
        FileReferenceFactory fileFactory = FileReferenceFactory.from(new PartitionsBuilder(schema).singlePartition("root").buildTree());
        committer.apply(StateStoreCommitRequest.create(tableId,
                AddFilesTransaction.fromReferences(
                        List.of(fileFactory.rootFile("prewarm-file.parquet", 123)))));

        return runRequestsGetStats(committer, IntStream.rangeClosed(1, numberOfRequests)
                .mapToObj(i -> StateStoreCommitRequest.create(tableId,
                        AddFilesTransaction.fromReferences(
                                List.of(fileFactory.rootFile("file-" + i + ".parquet", i))))));
    }

    private Stats runRequestsGetStats(StateStoreCommitter committer, Stream<StateStoreCommitRequest> requests) throws Exception {
        Instant startTime = Instant.now();
        AtomicInteger numRequestsTracker = new AtomicInteger();
        requests.forEach(request -> {
            committer.apply(request);
            numRequestsTracker.incrementAndGet();
        });
        Instant endTime = Instant.now();
        return new Stats(numRequestsTracker.get(), startTime, endTime);
    }

    private static class Stats {

        private final int numRequests;
        private final Instant startTime;
        private final Instant endTime;

        Stats(int numRequests, Instant startTime, Instant endTime) {
            this.numRequests = numRequests;
            this.startTime = startTime;
            this.endTime = endTime;
        }

        void log() {
            Duration duration = Duration.between(startTime, endTime);
            double durationSeconds = duration.toMillis() / 1000.0;
            Duration averageRequestDuration = duration.dividedBy(numRequests);
            LOGGER.info("Processed {} requests in {}", numRequests, LoggedDuration.withFullOutput(duration));
            LOGGER.info("Average rate of {}/s", numRequests / durationSeconds);
            LOGGER.info("Average request time: {}", LoggedDuration.withFullOutput(averageRequestDuration));
        }
    }

    private InstanceProperties createInstance() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        createBucket(instanceProperties.get(CONFIG_BUCKET));
        createBucket(instanceProperties.get(DATA_BUCKET));
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        return instanceProperties;
    }

    private TableProperties createTable(Schema schema) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient).createTable(tableProperties);
        update(stateStoreProvider().getStateStore(tableProperties)).initialise(schema);
        return tableProperties;
    }

    private StateStoreCommitter committer() {
        TablePropertiesProvider tablePropertiesProvider = tablePropertiesProvider();
        return new StateStoreCommitter(
                tablePropertiesProvider,
                stateStoreProvider(),
                new S3TransactionBodyStore(instanceProperties, s3Client, TransactionSerDeProvider.from(tablePropertiesProvider)));
    }

    private TablePropertiesProvider tablePropertiesProvider() {
        return S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
    }

    private StateStoreProvider stateStoreProvider() {
        return StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient);
    }

}
