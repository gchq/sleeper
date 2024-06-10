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

package sleeper.ingest.job;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.job.commit.IngestJobCommitRequest;
import sleeper.ingest.status.store.job.DynamoDBIngestJobStatusStoreCreator;
import sleeper.ingest.status.store.task.DynamoDBIngestTaskStatusStoreCreator;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.ingest.testutils.ResultVerifier;

import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.INGEST_JOB_COMMIT_ASYNC;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;
import static sleeper.ingest.job.IngestJobTestData.createJobWithTableAndFiles;
import static sleeper.ingest.task.IngestTaskStatusTestData.DEFAULT_FINISH_TIME;
import static sleeper.ingest.task.IngestTaskStatusTestData.DEFAULT_START_TIME;
import static sleeper.ingest.testutils.ResultVerifier.readMergedRecordsFromPartitionDataFiles;

public class ECSIngestTaskRunnerIT extends IngestJobQueueConsumerTestBase {

    @BeforeEach
    void setUp() {
        DynamoDBIngestTaskStatusStoreCreator.create(instanceProperties, dynamoDB);
        DynamoDBIngestJobStatusStoreCreator.create(instanceProperties, dynamoDB);
    }

    @Test
    public void shouldIngestParquetFilesPutOnTheQueue() throws Exception {
        // Given
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        List<String> files = writeParquetFilesForIngest(recordListAndSchema, "", 2);
        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .buildTree();
        List<Record> expectedRecords = Collections.nCopies(2, recordListAndSchema.recordList).stream()
                .flatMap(List::stream).collect(Collectors.toList());
        String localDir = createTempDirectory(temporaryFolder, null).toString();
        StateStore stateStore = createTable(recordListAndSchema.sleeperSchema);
        sendJobs(List.of(createJobWithTableAndFiles("job", tableProperties.getStatus(), files)));

        // When
        runTask(localDir, "task");

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree,
                actualFiles.get(0).getLastStateStoreUpdateTime());
        FileReference expectedFile = fileReferenceFactory.rootFile(actualFiles.get(0).getFilename(), 400);
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        assertThat(Paths.get(localDir)).isEmptyDirectory();
        assertThat(actualFiles).containsExactly(expectedFile);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);

        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration);
    }

    @Test
    public void shouldContinueReadingFromQueueWhileMoreMessagesExist() throws Exception {
        // Given
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        int noOfJobs = 10;
        int noOfFilesPerJob = 4;
        List<IngestJob> ingestJobs = IntStream.rangeClosed(1, noOfJobs)
                .mapToObj(jobNo -> "job-" + jobNo)
                .map(jobId -> IngestJob.builder().tableName(tableName).id(jobId)
                        .files(writeParquetFilesForIngest(recordListAndSchema, jobId, noOfFilesPerJob))
                        .build())
                .collect(Collectors.toList());
        List<Record> expectedRecords = Collections.nCopies(40, recordListAndSchema.recordList).stream()
                .flatMap(List::stream).collect(Collectors.toList());
        String localDir = createTempDirectory(temporaryFolder, null).toString();
        StateStore stateStore = createTable(recordListAndSchema.sleeperSchema);
        sendJobs(ingestJobs);

        // When
        runTask(localDir, "test-task");

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .buildTree();
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree,
                Instant.parse("2023-08-08T11:20:00Z"));

        assertThat(Paths.get(localDir)).isEmptyDirectory();
        assertThat(actualFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime", "filename")
                .containsExactlyElementsOf(Collections.nCopies(10,
                        fileReferenceFactory.rootFile("anyfilename", 800)));
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);
        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getField("key0").orElseThrow(),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration);
    }

    @Test
    void shouldSendCommitRequestToQueueIfAsyncCommitsEnabled() throws Exception {
        // Given
        tableProperties.set(INGEST_JOB_COMMIT_ASYNC, "true");
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        IngestJob ingestJob = IngestJob.builder().tableName(tableName).tableId(tableId).id("job-1")
                .files(writeParquetFilesForIngest(recordListAndSchema, "job-1", 1))
                .build();

        String localDir = createTempDirectory(temporaryFolder, null).toString();
        StateStore stateStore = createTable(recordListAndSchema.sleeperSchema);
        FileReference expectedFileReference = FileReferenceFactory.from(stateStore).rootFile(200);
        sendJobs(List.of(ingestJob));

        // When
        runTask(localDir, "test-task");

        // Then
        assertThat(messagesOnQueue(STATESTORE_COMMITTER_QUEUE_URL))
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("startTime", "finishTime", "fileReferenceList.filename")
                .containsExactly(new IngestJobCommitRequest(ingestJob, "test-task", List.of(expectedFileReference),
                        summary(DEFAULT_START_TIME, DEFAULT_FINISH_TIME, 200, 200)));
    }

    private void runTask(String localDir, String taskId) throws Exception {
        ECSIngestTaskRunner.createIngestTask(
                ObjectFactory.noUserJars(), instanceProperties, localDir, taskId,
                s3, dynamoDB, sqs, cloudWatch, s3Async, hadoopConfiguration)
                .run();
    }

    private void sendJobs(List<IngestJob> jobs) {
        jobs.forEach(job -> sqs.sendMessage(
                instanceProperties.get(INGEST_JOB_QUEUE_URL),
                new IngestJobSerDe().toJson(job)));
    }

    private Stream<IngestJobCommitRequest> messagesOnQueue(InstanceProperty queueProperty) {
        return sqs.receiveMessage(new ReceiveMessageRequest()
                .withQueueUrl(instanceProperties.get(queueProperty))
                .withAttributeNames("MessageGroupId")
                .withWaitTimeSeconds(2))
                .getMessages().stream()
                .map(Message::getBody)
                .map(commitRequestSerDe::fromJson);
    }
}
