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

package sleeper.ingest.job;

import org.junit.jupiter.api.Test;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.iterator.IteratorException;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.status.store.job.DynamoDBIngestJobStatusStoreCreator;
import sleeper.ingest.status.store.task.DynamoDBIngestTaskStatusStoreCreator;
import sleeper.ingest.task.IngestTask;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.ingest.testutils.ResultVerifier;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.ingest.job.IngestJobTestData.createJobWithTableAndFiles;
import static sleeper.ingest.testutils.ResultVerifier.readMergedRecordsFromPartitionDataFiles;

public class ECSIngestTaskIT extends IngestJobQueueConsumerTestBase {
    private IngestTask createTaskRunner(InstanceProperties instanceProperties,
                                        String localDir,
                                        String taskId) {
        return ECSIngestTask.createIngestTask(
                ObjectFactory.noUserJars(), instanceProperties, localDir, taskId,
                s3, dynamoDB, sqs, cloudWatch, s3Async, hadoopConfiguration);
    }

    @Test
    public void shouldIngestParquetFilesPutOnTheQueue() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        List<String> files = writeParquetFilesForIngest(recordListAndSchema, "", 2);
        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .buildTree();
        InstanceProperties instanceProperties = getInstanceProperties();
        StateStore stateStore = new StateStoreProvider(dynamoDB, instanceProperties).getStateStore(createTable(recordListAndSchema.sleeperSchema));
        stateStore.initialise();
        String localDir = createTempDirectory(temporaryFolder, null).toString();

        ingestRecords(
                files,
                localDir,
                instanceProperties
        );

        // Verify the results
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(Instant.ofEpochMilli(actualFiles.get(0).getLastStateStoreUpdateTime()))
                .schema(recordListAndSchema.sleeperSchema)
                .build();
        List<FileInfo> fileInfoList = List.of(
                fileInfoFactory.leafFile(actualFiles.get(0).getFilename(), 400, -100L, 99L)
        );
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        assertThat(Paths.get(localDir)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(fileInfoList);
        recordListAndSchema.recordList.addAll(recordListAndSchema.recordList);
        assertThat(actualRecords).hasSize(recordListAndSchema.recordList.size());
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);

        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
    }

    @Test
    public void shouldContinueReadingFromQueueWhileMoreMessagesExist() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        int noOfJobs = 10;
        int noOfFilesPerJob = 4;
        List<IngestJob> ingestJobs = IntStream.range(0, noOfJobs)
                .mapToObj(jobNo -> {
                    try {
                        List<String> files = writeParquetFilesForIngest(recordListAndSchema, "job-" + jobNo, noOfFilesPerJob);
                        return IngestJob.builder()
                                .tableName(tableName).id(UUID.randomUUID().toString()).files(files)
                                .build();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());
        List<Record> expectedRecords = Stream.of(Collections.nCopies(noOfJobs * noOfFilesPerJob, recordListAndSchema.recordList))
                .flatMap(List::stream)
                .flatMap(List::stream)
                .collect(Collectors.toList());
        ingestJobs.forEach(ingestJob -> sqs.sendMessage(
                getInstanceProperties().get(INGEST_JOB_QUEUE_URL),
                new IngestJobSerDe().toJson(ingestJob)));
        String localDir = createTempDirectory(temporaryFolder, null).toString();
        InstanceProperties instanceProperties = getInstanceProperties();
        StateStore stateStore = new StateStoreProvider(dynamoDB, instanceProperties).getStateStore(createTable(recordListAndSchema.sleeperSchema));
        stateStore.initialise();
        DynamoDBIngestTaskStatusStoreCreator.create(instanceProperties, dynamoDB);
        DynamoDBIngestJobStatusStoreCreator.create(instanceProperties, dynamoDB);
        IngestTask runner = createTaskRunner(instanceProperties, localDir, "test-task");
        runner.run();

        // Verify the results
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .buildTree();
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(Instant.parse("2023-08-08T11:20:00Z"))
                .schema(recordListAndSchema.sleeperSchema)
                .build();

        List<FileInfo> fileInfoList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            fileInfoFactory = fileInfoFactory.toBuilder().lastStateStoreUpdate(Instant.ofEpochMilli(actualFiles.get(i).getLastStateStoreUpdateTime())).build();
            fileInfoList.add(fileInfoFactory.leafFile(actualFiles.get(i).getFilename(), 800, -100L, 99L));
        }
        assertThat(Paths.get(localDir)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(fileInfoList);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);
        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
    }

    private void ingestRecords(
            List<String> files,
            String localDir,
            InstanceProperties instanceProperties

    ) throws IteratorException, StateStoreException, IOException {
        IngestJob ingestJob = createJobWithTableAndFiles("id", tableName, files);
        sqs.sendMessage(getInstanceProperties().get(INGEST_JOB_QUEUE_URL), new IngestJobSerDe().toJson(ingestJob));
        DynamoDBIngestTaskStatusStoreCreator.create(instanceProperties, dynamoDB);
        DynamoDBIngestJobStatusStoreCreator.create(instanceProperties, dynamoDB);
        IngestTask runner = createTaskRunner(instanceProperties, localDir, "test-task");
        runner.run();
    }
}
