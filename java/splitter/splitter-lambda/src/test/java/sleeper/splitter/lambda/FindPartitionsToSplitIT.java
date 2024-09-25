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
package sleeper.splitter.lambda;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.Message;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.statestore.FixedStateStoreProvider;
import sleeper.core.CommonTestConstants;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.IngestRecordsFromIterator;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.ParquetConfiguration;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.arraylist.ArrayListRecordBatchFactory;
import sleeper.splitter.find.FindPartitionsToSplit;
import sleeper.splitter.find.SplitPartitionJobDefinition;
import sleeper.splitter.find.SplitPartitionJobDefinitionSerDe;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.PartitionSplittingProperty.MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.statestore.testutils.StateStoreTestHelper.inMemoryStateStoreWithSinglePartition;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.parquetConfiguration;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.standardIngestCoordinator;

@Testcontainers
public class FindPartitionsToSplitIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.SQS);

    @TempDir
    public Path tempDir;

    private final AmazonSQS sqsClient = buildAwsV1Client(localStackContainer,
            LocalStackContainer.Service.SQS, AmazonSQSClientBuilder.standard());
    private static final Schema SCHEMA = Schema.builder().rowKeyFields(new Field("key", new IntType())).build();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, SCHEMA);
    private final StateStore stateStore = inMemoryStateStoreWithSinglePartition(SCHEMA);
    private final String tableId = tableProperties.get(TABLE_ID);
    private final TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(tableProperties);

    @BeforeEach
    void setUp() {
        String queueName = UUID.randomUUID().toString();
        CreateQueueResult queue = sqsClient.createQueue(queueName);
        instanceProperties.set(PARTITION_SPLITTING_JOB_QUEUE_URL, queue.getQueueUrl());
    }

    @Test
    public void shouldPutMessagesOnAQueueIfAPartitionSizeGoesBeyondThreshold() throws StateStoreException, IOException {
        // Given
        instanceProperties.setNumber(MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB, 10);
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 500);
        writeFiles(createEvenRecordList(100, 10));

        // When
        findPartitionsToSplit().run(tableProperties);

        // Then
        List<Message> messages = receivePartitionSplittingMessages();
        assertThat(messages).hasSize(1);

        SplitPartitionJobDefinition job = new SplitPartitionJobDefinitionSerDe(tablePropertiesProvider)
                .fromJson(messages.get(0).getBody());

        assertThat(job.getFileNames()).hasSize(10);
        assertThat(job.getTableId()).isEqualTo(tableId);
        assertThat(job.getPartition()).isEqualTo(stateStore.getAllPartitions().get(0));
    }

    @Test
    public void shouldNotPutMessagesOnAQueueIfPartitionsAreAllUnderThreshold() throws StateStoreException, IOException {
        // Given
        instanceProperties.setNumber(MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB, 10);
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 1001);
        writeFiles(createEvenRecordList(100, 10));

        // When
        findPartitionsToSplit().run(tableProperties);

        // The
        assertThat(receivePartitionSplittingMessages()).isEmpty();
    }

    @Test
    public void shouldLimitNumberOfFilesInJobAccordingToTheMaximum() throws IOException, StateStoreException {
        // Given
        instanceProperties.setNumber(MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB, 5);
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 500);
        writeFiles(createEvenRecordList(100, 10));

        // When
        findPartitionsToSplit().run(tableProperties);

        // Then
        List<Message> messages = receivePartitionSplittingMessages();
        assertThat(messages).hasSize(1);

        SplitPartitionJobDefinition job = new SplitPartitionJobDefinitionSerDe(tablePropertiesProvider)
                .fromJson(messages.get(0).getBody());

        assertThat(job.getFileNames()).hasSize(5);
        assertThat(job.getTableId()).isEqualTo(tableId);
        assertThat(job.getPartition()).isEqualTo(stateStore.getAllPartitions().get(0));
    }

    @Test
    public void shouldPrioritiseFilesContainingTheLargestNumberOfRecords() throws StateStoreException, IOException {
        // Given
        instanceProperties.setNumber(MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB, 5);
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 500);
        writeFiles(createAscendingRecordList(100, 10));

        // When
        findPartitionsToSplit().run(tableProperties);

        // Then
        List<Message> messages = receivePartitionSplittingMessages();
        assertThat(messages).hasSize(1);

        SplitPartitionJobDefinition job = new SplitPartitionJobDefinitionSerDe(tablePropertiesProvider)
                .fromJson(messages.get(0).getBody());

        assertThat(job.getFileNames()).hasSize(5);
        assertThat(job.getTableId()).isEqualTo(tableId);
        assertThat(job.getPartition()).isEqualTo(stateStore.getAllPartitions().get(0));

        List<FileReference> fileReferences = stateStore.getFileReferences();
        Optional<Long> numberOfRecords = job.getFileNames().stream().flatMap(fileName -> fileReferences.stream()
                .filter(fi -> fi.getFilename().equals(fileName))
                .map(FileReference::getNumberOfRecords)).reduce(Long::sum);

        // 109 + 108 + 107 + 106 + 105 = 535
        assertThat(numberOfRecords).contains(535L);
    }

    private FindPartitionsToSplit findPartitionsToSplit() {
        return new FindPartitionsToSplit(instanceProperties,
                new FixedStateStoreProvider(tableProperties, stateStore),
                new SqsSplitPartitionJobSender(tablePropertiesProvider, instanceProperties, sqsClient)::send);
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

    private void writeFiles(List<List<Record>> recordLists) {
        ParquetConfiguration parquetConfiguration = parquetConfiguration(SCHEMA, new Configuration());
        recordLists.forEach(list -> {
            try {
                File stagingArea = createTempDirectory(tempDir, null).toFile();
                File directory = createTempDirectory(tempDir, null).toFile();
                try (IngestCoordinator<Record> coordinator = standardIngestCoordinator(stateStore, SCHEMA,
                        ArrayListRecordBatchFactory.builder()
                                .parquetConfiguration(parquetConfiguration)
                                .localWorkingDirectory(stagingArea.getAbsolutePath())
                                .maxNoOfRecordsInMemory(1_000_000)
                                .maxNoOfRecordsInLocalStore(1000L)
                                .buildAcceptingRecords(),
                        DirectPartitionFileWriterFactory.from(parquetConfiguration,
                                "file://" + directory.getAbsolutePath()))) {
                    new IngestRecordsFromIterator(coordinator, list.iterator()).write();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private List<Message> receivePartitionSplittingMessages() {
        return sqsClient.receiveMessage(instanceProperties.get(PARTITION_SPLITTING_JOB_QUEUE_URL)).getMessages();
    }
}
