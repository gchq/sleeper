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
package sleeper.statestore.committer;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.S3TableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.statestore.StateStoreProvider;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.util.LoggedDuration;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

@Testcontainers
@Disabled("For manual testing")
public class StateStoreCommitterThroughputIT {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreCommitterThroughputIT.class);

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    private final AmazonS3 s3 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final InstanceProperties instanceProperties = createInstance();

    @Test
    void shouldSendManyAddFilesRequestsWithNoJob() throws Exception {
        Stats stats10 = runAddFilesRequestsWithNoJobGetStats(10);
        Stats stats200 = runAddFilesRequestsWithNoJobGetStats(200);
        Stats stats1000 = runAddFilesRequestsWithNoJobGetStats(1000);
        stats10.log();
        stats200.log();
        stats1000.log();
    }

    private Stats runAddFilesRequestsWithNoJobGetStats(int numberOfRequests) throws Exception {
        Schema schema = schemaWithKey("key", new StringType());
        String tableId = createTable(schema).get(TABLE_ID);
        StateStoreCommitter committer = committer();
        FileReferenceFactory fileFactory = FileReferenceFactory.from(new PartitionsBuilder(schema).singlePartition("root").buildTree());
        committer.apply(StateStoreCommitRequest.forIngestAddFiles(IngestAddFilesCommitRequest.builder()
                .tableId(tableId)
                .fileReferences(List.of(fileFactory.rootFile("prewarm-file.parquet", 123)))
                .build()));

        return runRequestsGetStats(committer, IntStream.rangeClosed(1, numberOfRequests)
                .mapToObj(i -> StateStoreCommitRequest.forIngestAddFiles(IngestAddFilesCommitRequest.builder()
                        .tableId(tableId)
                        .fileReferences(List.of(fileFactory.rootFile("file-" + i + ".parquet", i)))
                        .build())));
    }

    private Stats runRequestsGetStats(StateStoreCommitter committer, Stream<StateStoreCommitRequest> requests) throws Exception {
        Instant startTime = Instant.now();
        AtomicInteger numRequestsTracker = new AtomicInteger();
        requests.forEach(request -> {
            try {
                committer.apply(request);
                numRequestsTracker.incrementAndGet();
            } catch (StateStoreException e) {
                throw new RuntimeException(e);
            }
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
        s3.createBucket(instanceProperties.get(CONFIG_BUCKET));
        s3.createBucket(instanceProperties.get(DATA_BUCKET));
        instanceProperties.saveToS3(s3);
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoDB).create();
        return instanceProperties;
    }

    private TableProperties createTable(Schema schema) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        S3TableProperties.getStore(instanceProperties, s3, dynamoDB).createTable(tableProperties);
        try {
            stateStoreProvider().getStateStore(tableProperties).initialise();
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
        return tableProperties;
    }

    private StateStoreCommitter committer() {
        return new StateStoreCommitter(
                CompactionJobStatusStoreFactory.getStatusStore(dynamoDB, instanceProperties),
                IngestJobStatusStoreFactory.getStatusStore(dynamoDB, instanceProperties),
                tablePropertiesProvider(),
                stateStoreProvider(),
                Instant::now);
    }

    private TablePropertiesProvider tablePropertiesProvider() {
        return new TablePropertiesProvider(instanceProperties, s3, dynamoDB);
    }

    private StateStoreProvider stateStoreProvider() {
        return StateStoreFactory.createProvider(instanceProperties, s3, dynamoDB,
                HadoopConfigurationLocalStackUtils.getHadoopConfiguration(localStackContainer));
    }

}
