/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.splitter;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.Message;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.ingest.IngestRecordsFromIterator;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.ParquetConfiguration;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.arraylist.ArrayListRecordBatchFactory;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.parquetConfiguration;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.standardIngestCoordinator;

@Testcontainers
public class FindPartitionsToSplitIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.SQS);

    @TempDir
    public Path tempDir;

    private static final Schema SCHEMA = Schema.builder().rowKeyFields(new Field("key", new IntType())).build();

    private AmazonDynamoDB createDynamoClient() {
        return buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    }

    private AmazonSQS createSQSClient() {
        return buildAwsV1Client(localStackContainer, LocalStackContainer.Service.SQS, AmazonSQSClientBuilder.standard());
    }

    private StateStore createStateStore(AmazonDynamoDB dynamoDB) throws StateStoreException {
        return createStateStore(dynamoDB, new PartitionsFromSplitPoints(SCHEMA, Collections.emptyList()).construct());
    }

    private StateStore createStateStore(AmazonDynamoDB dynamoDB, List<Partition> partitions) throws StateStoreException {
        String id = UUID.randomUUID().toString();
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator(id, SCHEMA, dynamoDB);
        StateStore dynamoStateStore = dynamoDBStateStoreCreator.create();
        dynamoStateStore.initialise(partitions);
        return dynamoStateStore;
    }

    private List<List<Record>> createEvenRecordList(Integer recordsPerList, Integer numberOfLists) {
        List<List<Record>> recordLists = new ArrayList<>();
        for (int i = 0; i < numberOfLists; i++) {
            List<Record> records = new ArrayList<>();
            for (int j = 0; j < recordsPerList; j++) {
                Record record = new Record();
                record.put("key", j);
                records.add(record);
            }
            recordLists.add(records);
        }

        return recordLists;
    }

    private List<List<Record>> createAscendingRecordList(Integer startingRecordsPerList, Integer numberOfLists) {
        List<List<Record>> recordLists = new ArrayList<>();
        Integer recordsPerList = startingRecordsPerList;
        for (int i = 0; i < numberOfLists; i++) {
            List<Record> records = new ArrayList<>();
            for (int j = 0; j < recordsPerList; j++) {
                Record record = new Record();
                record.put("key", j);
                records.add(record);
            }
            recordLists.add(records);
            recordsPerList++;
        }

        return recordLists;
    }

    private void writeFiles(StateStore stateStore, Schema schema, List<List<Record>> recordLists) {
        ParquetConfiguration parquetConfiguration = parquetConfiguration(schema, new Configuration());
        recordLists.forEach(list -> {
            try {
                File stagingArea = createTempDirectory(tempDir, null).toFile();
                File directory = createTempDirectory(tempDir, null).toFile();
                try (IngestCoordinator<Record> coordinator = standardIngestCoordinator(stateStore, schema,
                        ArrayListRecordBatchFactory.builder()
                                .parquetConfiguration(parquetConfiguration)
                                .localWorkingDirectory(stagingArea.getAbsolutePath())
                                .maxNoOfRecordsInMemory(1_000_000)
                                .maxNoOfRecordsInLocalStore(1000L)
                                .buildAcceptingRecords(),
                        DirectPartitionFileWriterFactory.from(parquetConfiguration,
                                "file://" + directory.getAbsolutePath())
                )) {
                    new IngestRecordsFromIterator(coordinator, list.iterator()).write();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void shouldPutMessagesOnAQueueIfAPartitionSizeGoesBeyondThreshold() throws StateStoreException, IOException {
        // Given
        AmazonDynamoDB dynamoClient = createDynamoClient();
        AmazonSQS sqsClient = createSQSClient();
        CreateQueueResult queue = sqsClient.createQueue(UUID.randomUUID().toString());
        TablePropertiesProvider tablePropertiesProvider = new TestTablePropertiesProvider(SCHEMA, 500);
        StateStore stateStore = createStateStore(dynamoClient);
        writeFiles(stateStore, SCHEMA, createEvenRecordList(100, 10));

        // When
        FindPartitionsToSplit partitionFinder = new FindPartitionsToSplit("test", tablePropertiesProvider,
                stateStore, 10, sqsClient, queue.getQueueUrl());
        partitionFinder.run();

        // Then
        List<Message> messages = sqsClient.receiveMessage(queue.getQueueUrl()).getMessages();
        assertThat(messages).hasSize(1);

        SplitPartitionJobDefinition job = new SplitPartitionJobDefinitionSerDe(tablePropertiesProvider)
                .fromJson(messages.get(0).getBody());

        assertThat(job.getFileNames()).hasSize(10);
        assertThat(job.getTableName()).isEqualTo("test");
        assertThat(job.getPartition()).isEqualTo(stateStore.getAllPartitions().get(0));
    }

    @Test
    public void shouldNotPutMessagesOnAQueueIfPartitionsAreAllUnderThreshold() throws StateStoreException, IOException {
        // Given
        AmazonDynamoDB dynamoClient = createDynamoClient();
        AmazonSQS sqsClient = createSQSClient();
        CreateQueueResult queue = sqsClient.createQueue(UUID.randomUUID().toString());
        TablePropertiesProvider tablePropertiesProvider = new TestTablePropertiesProvider(SCHEMA, 1001);
        StateStore stateStore = createStateStore(dynamoClient);
        writeFiles(stateStore, SCHEMA, createEvenRecordList(100, 10));

        // When
        FindPartitionsToSplit partitionFinder = new FindPartitionsToSplit("test", tablePropertiesProvider,
                stateStore, 10, sqsClient, queue.getQueueUrl());
        partitionFinder.run();

        // Then
        List<Message> messages = sqsClient.receiveMessage(queue.getQueueUrl()).getMessages();
        assertThat(messages).isEmpty();
    }

    @Test
    public void shouldLimitNumberOfFilesInJobAccordingToTheMaximum() throws IOException, StateStoreException {
        // Given
        AmazonDynamoDB dynamoClient = createDynamoClient();
        AmazonSQS sqsClient = createSQSClient();
        CreateQueueResult queue = sqsClient.createQueue(UUID.randomUUID().toString());
        TablePropertiesProvider tablePropertiesProvider = new TestTablePropertiesProvider(SCHEMA, 500);
        StateStore stateStore = createStateStore(dynamoClient);
        writeFiles(stateStore, SCHEMA, createEvenRecordList(100, 10));

        // When
        FindPartitionsToSplit partitionFinder = new FindPartitionsToSplit("test", tablePropertiesProvider,
                stateStore, 5, sqsClient, queue.getQueueUrl());
        partitionFinder.run();

        // Then
        List<Message> messages = sqsClient.receiveMessage(queue.getQueueUrl()).getMessages();
        assertThat(messages).hasSize(1);

        SplitPartitionJobDefinition job = new SplitPartitionJobDefinitionSerDe(tablePropertiesProvider)
                .fromJson(messages.get(0).getBody());

        assertThat(job.getFileNames()).hasSize(5);
        assertThat(job.getTableName()).isEqualTo("test");
        assertThat(job.getPartition()).isEqualTo(stateStore.getAllPartitions().get(0));
    }

    @Test
    public void shouldPrioritiseFilesContainingTheLargestNumberOfRecords() throws StateStoreException, IOException {
        // Given
        AmazonDynamoDB dynamoClient = createDynamoClient();
        AmazonSQS sqsClient = createSQSClient();
        CreateQueueResult queue = sqsClient.createQueue(UUID.randomUUID().toString());
        TablePropertiesProvider tablePropertiesProvider = new TestTablePropertiesProvider(SCHEMA, 500);
        StateStore stateStore = createStateStore(dynamoClient);
        writeFiles(stateStore, SCHEMA, createAscendingRecordList(100, 10));

        // When
        FindPartitionsToSplit partitionFinder = new FindPartitionsToSplit("test", tablePropertiesProvider,
                stateStore, 5, sqsClient, queue.getQueueUrl());
        partitionFinder.run();

        // Then
        List<Message> messages = sqsClient.receiveMessage(queue.getQueueUrl()).getMessages();
        assertThat(messages).hasSize(1);

        SplitPartitionJobDefinition job = new SplitPartitionJobDefinitionSerDe(tablePropertiesProvider)
                .fromJson(messages.get(0).getBody());

        assertThat(job.getFileNames()).hasSize(5);
        assertThat(job.getTableName()).isEqualTo("test");
        assertThat(job.getPartition()).isEqualTo(stateStore.getAllPartitions().get(0));

        List<FileInfo> fileInPartitionList = stateStore.getFileInPartitionList();
        Optional<Long> numberOfRecords = job.getFileNames().stream().flatMap(fileName -> fileInPartitionList.stream()
                .filter(fi -> fi.getFilename().equals(fileName))
                .map(FileInfo::getNumberOfRecords)).reduce(Long::sum);

        // 109 + 108 + 107 + 106 + 105 = 535
        assertThat(numberOfRecords).contains(Long.valueOf(535L));
    }

    @Test
    public void shouldOnlyIncludeFilesThatOnlyContainDataForThePartition() throws StateStoreException, IOException {
        // Given
        AmazonDynamoDB dynamoClient = createDynamoClient();
        AmazonSQS sqsClient = createSQSClient();
        CreateQueueResult queue = sqsClient.createQueue(UUID.randomUUID().toString());
        TablePropertiesProvider tablePropertiesProvider = new TestTablePropertiesProvider(SCHEMA, 500);
        StateStore stateStore = createStateStore(dynamoClient);
        writeFiles(stateStore, SCHEMA, createAscendingRecordList(100, 10));
        stateStore.getFileInPartitionList().forEach(System.out::println);

        // - Split the root partition
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        rootPartition.setLeafPartition(false);
        rootPartition.setChildPartitionIds(Arrays.asList("left", "right"));
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);
        Range leftRange = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), Integer.MIN_VALUE, 50);
        Region leftRegion = new Region(leftRange);
        Partition leftLeafPartition = Partition.builder()
            .rowKeyTypes(SCHEMA.getRowKeyTypes())
            .id("left")
            .region(leftRegion)
            .leafPartition(true)
            .parentPartitionId(rootPartition.getId())
            .build();
        Range rightRange = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 50, null);
        Region rightRegion = new Region(rightRange);
        Partition rightLeafPartition = Partition.builder()
            .rowKeyTypes(SCHEMA.getRowKeyTypes())
            .id("right")
            .region(rightRegion)
            .leafPartition(true)
            .parentPartitionId(rootPartition.getId())
            .build();
        stateStore.atomicallyUpdatePartitionAndCreateNewOnes(rootPartition, leftLeafPartition, rightLeafPartition);
        writeFiles(stateStore, SCHEMA, createAscendingRecordList(100, 10));

        // When
        FindPartitionsToSplit partitionFinder = new FindPartitionsToSplit("test", tablePropertiesProvider,
                stateStore, 5, sqsClient, queue.getQueueUrl());
        partitionFinder.run();

        // Then
        // - Make 2 calls to receiveMessage to get both messages
        List<Message> messages = sqsClient.receiveMessage(queue.getQueueUrl()).getMessages();
        messages.addAll(sqsClient.receiveMessage(queue.getQueueUrl()).getMessages());
        assertThat(messages).hasSize(2);

        SplitPartitionJobDefinition job1 = new SplitPartitionJobDefinitionSerDe(tablePropertiesProvider)
                .fromJson(messages.get(0).getBody());
        SplitPartitionJobDefinition job2 = new SplitPartitionJobDefinitionSerDe(tablePropertiesProvider)
                .fromJson(messages.get(1).getBody());

        assertThat(job1.getFileNames()).hasSize(5);
        assertThat(job1.getTableName()).isEqualTo("test");
        Set<String> partitions = new HashSet<>();
        partitions.add(job1.getPartition().getId());
        partitions.add(job2.getPartition().getId());
        assertThat(partitions).containsExactlyInAnyOrder("left", "right");

        assertThat(job1.getFileNames().stream().anyMatch(s -> s.contains(rootPartition.getId()))).isFalse();
        assertThat(job2.getFileNames().stream().anyMatch(s -> s.contains(rootPartition.getId()))).isFalse();
    }

    public static class TestTablePropertiesProvider extends TablePropertiesProvider {
        private final Schema schema;
        private final long splitThreshold;

        TestTablePropertiesProvider(Schema schema, long splitThreshold) {
            super(null, null);
            this.schema = schema;
            this.splitThreshold = splitThreshold;
        }

        TestTablePropertiesProvider(Schema schema) {
            this(schema, 1_000_000_000L);
        }

        @Override
        public TableProperties getTableProperties(String tableName) {
            TableProperties tableProperties = new TableProperties(new InstanceProperties());
            tableProperties.setSchema(schema);
            tableProperties.set(PARTITION_SPLIT_THRESHOLD, "" + splitThreshold);
            return tableProperties;
        }
    }
}
