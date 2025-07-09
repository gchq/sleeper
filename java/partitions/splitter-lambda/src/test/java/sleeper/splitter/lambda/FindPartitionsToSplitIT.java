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
package sleeper.splitter.lambda;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.Message;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.record.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.table.TableFilePaths;
import sleeper.ingest.runner.IngestRecordsFromIterator;
import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.ingest.runner.impl.ParquetConfiguration;
import sleeper.ingest.runner.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.runner.impl.recordbatch.arraylist.ArrayListRecordBatchFactory;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.sketches.store.LocalFileSystemSketchesStore;
import sleeper.splitter.core.find.FindPartitionsToSplit;
import sleeper.splitter.core.find.SplitPartitionJobDefinition;
import sleeper.splitter.core.find.SplitPartitionJobDefinitionSerDe;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.PartitionSplittingProperty.MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.ingest.runner.testutils.IngestCoordinatorTestHelper.parquetConfiguration;
import static sleeper.ingest.runner.testutils.IngestCoordinatorTestHelper.standardIngestCoordinator;

public class FindPartitionsToSplitIT extends LocalStackTestBase {

    @TempDir
    public Path tempDir;

    private static final Schema SCHEMA = Schema.builder().rowKeyFields(new Field("key", new IntType())).build();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, SCHEMA);
    private final StateStore stateStore = InMemoryTransactionLogStateStore.createAndInitialise(tableProperties, new InMemoryTransactionLogs());
    private final String tableId = tableProperties.get(TABLE_ID);
    private final TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(tableProperties);

    @BeforeEach
    void setUp() {
        String queueName = UUID.randomUUID().toString();
        CreateQueueResponse queue = sqsClient.createQueue(request -> request
                .queueName(queueName));
        instanceProperties.set(PARTITION_SPLITTING_JOB_QUEUE_URL, queue.queueUrl());
    }

    @Test
    public void shouldPutMessagesOnAQueueIfAPartitionSizeGoesBeyondThreshold() throws IOException {
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
                .fromJson(messages.get(0).body());

        assertThat(job.getFileNames()).hasSize(10);
        assertThat(job.getTableId()).isEqualTo(tableId);
        assertThat(job.getPartition()).isEqualTo(stateStore.getAllPartitions().get(0));
    }

    @Test
    public void shouldNotPutMessagesOnAQueueIfPartitionsAreAllUnderThreshold() throws IOException {
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
    public void shouldLimitNumberOfFilesInJobAccordingToTheMaximum() throws IOException {
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
                .fromJson(messages.get(0).body());

        assertThat(job.getFileNames()).hasSize(5);
        assertThat(job.getTableId()).isEqualTo(tableId);
        assertThat(job.getPartition()).isEqualTo(stateStore.getAllPartitions().get(0));
    }

    @Test
    public void shouldPrioritiseFilesContainingTheLargestNumberOfRecords() throws IOException {
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
                .fromJson(messages.get(0).body());

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

    private List<List<Row>> createEvenRecordList(Integer recordsPerList, Integer numberOfLists) {
        List<List<Row>> recordLists = new ArrayList<>();
        for (int i = 0; i < numberOfLists; i++) {
            List<Row> records = new ArrayList<>();
            for (int j = 0; j < recordsPerList; j++) {
                Row record = new Row();
                record.put("key", j);
                records.add(record);
            }
            recordLists.add(records);
        }

        return recordLists;
    }

    private List<List<Row>> createAscendingRecordList(Integer startingRecordsPerList, Integer numberOfLists) {
        List<List<Row>> recordLists = new ArrayList<>();
        Integer recordsPerList = startingRecordsPerList;
        for (int i = 0; i < numberOfLists; i++) {
            List<Row> records = new ArrayList<>();
            for (int j = 0; j < recordsPerList; j++) {
                Row record = new Row();
                record.put("key", j);
                records.add(record);
            }
            recordLists.add(records);
            recordsPerList++;
        }

        return recordLists;
    }

    private void writeFiles(List<List<Row>> recordLists) {
        ParquetConfiguration parquetConfiguration = parquetConfiguration(SCHEMA, new Configuration());
        recordLists.forEach(list -> {
            try {
                File stagingArea = createTempDirectory(tempDir, null).toFile();
                File directory = createTempDirectory(tempDir, null).toFile();
                try (IngestCoordinator<Row> coordinator = standardIngestCoordinator(stateStore, SCHEMA,
                        ArrayListRecordBatchFactory.builder()
                                .parquetConfiguration(parquetConfiguration)
                                .localWorkingDirectory(stagingArea.getAbsolutePath())
                                .maxNoOfRecordsInMemory(1_000_000)
                                .maxNoOfRecordsInLocalStore(1000L)
                                .buildAcceptingRecords(),
                        DirectPartitionFileWriterFactory.builder()
                                .parquetConfiguration(parquetConfiguration)
                                .filePaths(TableFilePaths.fromPrefix("file://" + directory.getAbsolutePath()))
                                .sketchesStore(new LocalFileSystemSketchesStore())
                                .build())) {
                    new IngestRecordsFromIterator(coordinator, list.iterator()).write();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private List<Message> receivePartitionSplittingMessages() {
        return sqsClient.receiveMessage(builder -> builder
                .queueUrl(instanceProperties.get(PARTITION_SPLITTING_JOB_QUEUE_URL)))
                .messages();
    }
}
