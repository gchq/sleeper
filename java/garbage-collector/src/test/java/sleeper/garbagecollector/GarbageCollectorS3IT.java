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

package sleeper.garbagecollector;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.commit.GarbageCollectionCommitRequest;
import sleeper.core.statestore.commit.GarbageCollectionCommitRequestSerDe;
import sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils;
import sleeper.statestore.FixedStateStoreProvider;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_ASYNC_COMMIT;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.FilesReportTestHelper.activeAndReadyForGCFilesReport;
import static sleeper.core.statestore.ReplaceFileReferencesRequest.replaceJobFileReferences;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithSinglePartition;

@Testcontainers
public class GarbageCollectorS3IT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(Service.S3, Service.SQS);
    private final AmazonS3 s3Client = buildAwsV1Client(localStackContainer, Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonSQS sqsClient = buildAwsV1Client(localStackContainer, Service.SQS, AmazonSQSClientBuilder.standard());

    private static final Schema TEST_SCHEMA = getSchema();
    private final PartitionTree partitions = new PartitionsBuilder(TEST_SCHEMA).singlePartition("root").buildTree();
    private final FileReferenceFactory factory = FileReferenceFactory.from(partitions);
    private final Configuration configuration = HadoopConfigurationLocalStackUtils.getHadoopConfiguration(localStackContainer);
    private final String testBucket = UUID.randomUUID().toString();
    private final InstanceProperties instanceProperties = createInstanceProperties();

    @BeforeEach
    void setUp() {
        s3Client.createBucket(testBucket);
    }

    StateStore setupStateStoreAndFixTime(Instant fixedTime) {
        StateStore stateStore = inMemoryStateStoreWithSinglePartition(TEST_SCHEMA);
        stateStore.fixFileUpdateTime(fixedTime);
        return stateStore;
    }

    @Test
    void shouldContinueCollectingFilesIfTryingToDeleteFileThrowsIOException() throws Exception {
        // Given
        TableProperties tableProperties = createTableWithGCDelay(instanceProperties, 10);
        Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
        Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
        StateStore stateStore = setupStateStoreAndFixTime(oldEnoughTime);
        // Create a FileReference referencing a file in a bucket that does not exist
        FileReference oldFile1 = factory.rootFile("s3a://not-a-bucket/old-file-1.parquet", 100L);
        stateStore.addFile(oldFile1);
        // Perform a compaction on an existing file to create a readyForGC file
        s3Client.putObject(testBucket, "old-file-2.parquet", "abc");
        s3Client.putObject(testBucket, "new-file-2.parquet", "def");
        FileReference oldFile2 = factory.rootFile("s3a://" + testBucket + "/old-file-2.parquet", 100L);
        FileReference newFile2 = factory.rootFile("s3a://" + testBucket + "/new-file-2.parquet", 100L);
        stateStore.addFile(oldFile2);
        stateStore.assignJobIds(List.of(
                assignJobOnPartitionToFiles("job1", "root",
                        List.of(oldFile1.getFilename(), oldFile2.getFilename()))));
        stateStore.atomicallyReplaceFileReferencesWithNewOnes(List.of(replaceJobFileReferences(
                "job1", "root", List.of(oldFile1.getFilename(), oldFile2.getFilename()), newFile2)));

        // When
        GarbageCollector collector = createGarbageCollector(instanceProperties, tableProperties, stateStore);

        // And / Then
        assertThatThrownBy(() -> collector.runAtTime(currentTime, List.of(tableProperties)))
                .isInstanceOf(FailedGarbageCollectionException.class);
        assertThat(s3Client.doesObjectExist(testBucket, "old-file-2.parquet")).isFalse();
        assertThat(s3Client.doesObjectExist(testBucket, "new-file-2.parquet")).isTrue();
        assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                .isEqualTo(activeAndReadyForGCFilesReport(oldEnoughTime,
                        List.of(newFile2),
                        List.of(oldFile1.getFilename())));
    }

    @Test
    void shouldSendAsyncCommit() throws Exception {
        // Given
        TableProperties tableProperties = createTableWithGCDelay(instanceProperties, 10);
        tableProperties.set(GARBAGE_COLLECTOR_ASYNC_COMMIT, "true");
        Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
        Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
        StateStore stateStore = setupStateStoreAndFixTime(oldEnoughTime);
        // Perform a compaction on an existing file to create a readyForGC file
        s3Client.putObject(testBucket, "old-file.parquet", "abc");
        s3Client.putObject(testBucket, "new-file.parquet", "def");
        FileReference oldFile = factory.rootFile("s3a://" + testBucket + "/old-file.parquet", 100L);
        FileReference newFile = factory.rootFile("s3a://" + testBucket + "/new-file.parquet", 100L);
        stateStore.addFile(oldFile);
        stateStore.assignJobIds(List.of(
                assignJobOnPartitionToFiles("test-job", "root", List.of(oldFile.getFilename()))));
        stateStore.atomicallyReplaceFileReferencesWithNewOnes(List.of(replaceJobFileReferences(
                "test-job", "root", List.of(oldFile.getFilename()), newFile)));

        // When
        createGarbageCollector(instanceProperties, tableProperties, stateStore)
                .runAtTime(currentTime, List.of(tableProperties));

        // Then
        assertThat(s3Client.doesObjectExist(testBucket, "old-file.parquet")).isFalse();
        assertThat(s3Client.doesObjectExist(testBucket, "new-file.parquet")).isTrue();
        assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                .isEqualTo(activeAndReadyForGCFilesReport(oldEnoughTime, List.of(newFile), List.of(oldFile.getFilename())));
        assertThat(receiveGarbageCollectionCommitRequests())
                .containsExactly(new GarbageCollectionCommitRequest(tableProperties.get(TABLE_ID), List.of(oldFile.getFilename())));
    }

    private InstanceProperties createInstanceProperties() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, createFifoQueueGetUrl());
        instanceProperties.set(FILE_SYSTEM, "s3a://");
        instanceProperties.set(DATA_BUCKET, testBucket);
        return instanceProperties;
    }

    private TableProperties createTableWithGCDelay(InstanceProperties instanceProperties, int gcDelay) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, TEST_SCHEMA);
        tableProperties.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, gcDelay);
        return tableProperties;
    }

    private GarbageCollector createGarbageCollector(InstanceProperties instanceProperties, TableProperties tableProperties, StateStore stateStore) {
        return new GarbageCollector(configuration, instanceProperties, new FixedStateStoreProvider(tableProperties, stateStore), sqsClient);
    }

    private static Schema getSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new StringType()))
                .build();
    }

    private String createFifoQueueGetUrl() {
        return sqsClient.createQueue(new CreateQueueRequest()
                .withQueueName(UUID.randomUUID().toString() + ".fifo")
                .withAttributes(Map.of("FifoQueue", "true"))).getQueueUrl();
    }

    private List<GarbageCollectionCommitRequest> receiveGarbageCollectionCommitRequests() {
        return receiveCommitMessages().stream()
                .map(message -> new GarbageCollectionCommitRequestSerDe().fromJson(message.getBody()))
                .collect(Collectors.toList());
    }

    private List<Message> receiveCommitMessages() {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                .withMaxNumberOfMessages(10);
        return sqsClient.receiveMessage(receiveMessageRequest).getMessages();
    }
}
