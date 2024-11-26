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
package sleeper.compaction.core.job.creation;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.core.job.commit.CompactionJobIdAssignmentCommitRequest;
import sleeper.compaction.core.job.creation.CreateCompactionJobs.BatchJobsWriter;
import sleeper.compaction.core.job.creation.CreateCompactionJobs.GenerateBatchId;
import sleeper.compaction.core.job.creation.CreateCompactionJobs.GenerateJobId;
import sleeper.compaction.core.job.creation.strategy.impl.BasicCompactionStrategy;
import sleeper.compaction.core.job.creation.strategy.impl.SizeRatioCompactionStrategy;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequest;
import sleeper.compaction.core.testutils.InMemoryCompactionJobStatusStore;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.core.util.ObjectFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LIMIT;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_ID_ASSIGNMENT_COMMIT_ASYNC;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_SEND_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.core.statestore.FileReferenceTestData.splitFile;
import static sleeper.core.statestore.FileReferenceTestData.withJobId;
import static sleeper.core.statestore.SplitFileReference.referenceForChildPartition;
import static sleeper.core.statestore.testutils.StateStoreTestHelper.inMemoryStateStoreUninitialised;
import static sleeper.core.testutils.SupplierTestHelper.fixIds;
import static sleeper.core.testutils.SupplierTestHelper.timePassesAMinuteAtATimeFrom;

public class CreateCompactionJobsTest {

    InstanceProperties instanceProperties = CreateJobsTestUtils.createInstanceProperties();
    Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
    InMemoryCompactionJobStatusStore jobStatusStore = new InMemoryCompactionJobStatusStore();
    TableProperties tableProperties = createTable();
    StateStore stateStore = createStateStore(tableProperties);

    List<CompactionJob> jobs = new ArrayList<>();
    Queue<CompactionJobDispatchRequest> pendingQueue = new LinkedList<>();
    Map<String, List<CompactionJob>> bucketAndKeyToJobs = new HashMap<>();
    List<CompactionJobIdAssignmentCommitRequest> jobIdAssignmentCommitRequests = new ArrayList<>();

    @Nested
    @DisplayName("Compact files using strategy")
    class CompactFilesByStrategy {

        @Test
        public void shouldCompactAllFilesInSinglePartition() throws Exception {
            // Given
            tableProperties.set(COMPACTION_STRATEGY_CLASS, SizeRatioCompactionStrategy.class.getName());
            stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            FileReference fileReference1 = fileFactory().rootFile("file1", 200L);
            FileReference fileReference2 = fileFactory().rootFile("file2", 200L);
            FileReference fileReference3 = fileFactory().rootFile("file3", 200L);
            FileReference fileReference4 = fileFactory().rootFile("file4", 200L);
            List<FileReference> fileReferences = List.of(fileReference1, fileReference2, fileReference3, fileReference4);
            stateStore.addFiles(fileReferences);

            // When
            createJobsWithStrategy(fixJobIds("test-job"));

            // Then
            assertThat(jobs).containsExactly(
                    compactionFactory().createCompactionJob("test-job", fileReferences, "root"));
        }

        @Test
        public void shouldCompactFilesInDifferentPartitions() throws Exception {
            // Given
            tableProperties.set(COMPACTION_STRATEGY_CLASS, SizeRatioCompactionStrategy.class.getName());
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("A")
                    .splitToNewChildren("A", "B", "C", "ddd")
                    .buildList());
            FileReference fileReference1 = fileFactory().partitionFile("B", "file1", 200L);
            FileReference fileReference2 = fileFactory().partitionFile("B", "file2", 200L);
            FileReference fileReference3 = fileFactory().partitionFile("C", "file3", 200L);
            FileReference fileReference4 = fileFactory().partitionFile("C", "file4", 200L);
            stateStore.addFiles(List.of(fileReference1, fileReference2, fileReference3, fileReference4));

            // When
            createJobsWithStrategy(fixJobIds("partition-b-job", "partition-c-job"));

            // Then
            assertThat(jobs).containsExactly(
                    compactionFactory().createCompactionJob("partition-b-job", List.of(fileReference1, fileReference2), "B"),
                    compactionFactory().createCompactionJob("partition-c-job", List.of(fileReference3, fileReference4), "C"));
        }
    }

    @Nested
    @DisplayName("Handle files split into multiple references")
    class HandleSplitFileReferences {

        @Test
        public void shouldPreSplitFilesOneLevelDownPartitionTreeBeforeCreatingNoCompactionJobs() throws Exception {
            // Given
            tableProperties.set(COMPACTION_STRATEGY_CLASS, SizeRatioCompactionStrategy.class.getName());
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("A")
                    .splitToNewChildren("A", "B", "C", "ddd")
                    .splitToNewChildren("B", "B1", "B2", "aaa")
                    .splitToNewChildren("C", "C1", "C2", "fff")
                    .buildList());
            FileReference fileReference1 = fileFactory().partitionFile("A", "file1", 200L);
            FileReference fileReference2 = fileFactory().partitionFile("A", "file2", 200L);
            stateStore.addFiles(List.of(fileReference1, fileReference2));

            // When
            createJobsWithStrategy(fixJobIds("partition-b-job", "partition-c-job"));

            // Then
            assertThat(jobs).isEmpty();
            assertThat(stateStore.getFileReferences()).containsExactlyInAnyOrder(
                    splitFile(fileReference1, "B"),
                    splitFile(fileReference2, "B"),
                    splitFile(fileReference1, "C"),
                    splitFile(fileReference2, "C"));
        }

        @Test
        public void shouldPreSplitFilesOneLevelDownPartitionTreeBeforeCreatingCompactionJobsOnLeafPartitions() throws Exception {
            // Given
            tableProperties.set(COMPACTION_STRATEGY_CLASS, SizeRatioCompactionStrategy.class.getName());
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("A")
                    .splitToNewChildren("A", "B", "C", "ddd")
                    .buildList());
            FileReference fileReference1 = fileFactory().partitionFile("A", "file1", 200L);
            FileReference fileReference2 = fileFactory().partitionFile("A", "file2", 200L);
            stateStore.addFiles(List.of(fileReference1, fileReference2));

            // When
            createJobsWithStrategy(fixJobIds("partition-b-job", "partition-c-job"));

            // Then
            assertThat(jobs).containsExactly(
                    compactionFactory().createCompactionJobWithFilenames("partition-b-job", List.of("file1", "file2"), "B"),
                    compactionFactory().createCompactionJobWithFilenames("partition-c-job", List.of("file1", "file2"), "C"));
            assertThat(stateStore.getFileReferences()).containsExactlyInAnyOrder(
                    withJobId("partition-b-job", splitFile(fileReference1, "B")),
                    withJobId("partition-b-job", splitFile(fileReference2, "B")),
                    withJobId("partition-c-job", splitFile(fileReference1, "C")),
                    withJobId("partition-c-job", splitFile(fileReference2, "C")));
        }

        @Test
        public void shouldCreateCompactionJobsToConvertSplitFilesToWholeFiles() throws Exception {
            // Given
            tableProperties.set(COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName());
            tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "1");
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("A")
                    .splitToNewChildren("A", "B", "C", "ddd")
                    .buildList());
            FileReference fileReference = fileFactory().partitionFile("A", "file", 200L);
            FileReference leftReference = splitFile(fileReference, "B");
            FileReference rightReference = splitFile(fileReference, "C");
            stateStore.addFiles(List.of(leftReference, rightReference));

            // When
            createJobsWithStrategy(fixJobIds("partition-b-job", "partition-c-job"));

            // Then
            assertThat(jobs).containsExactly(
                    compactionFactory().createCompactionJobWithFilenames("partition-b-job", List.of("file"), "B"),
                    compactionFactory().createCompactionJobWithFilenames("partition-c-job", List.of("file"), "C"));
            assertThat(stateStore.getFileReferences()).containsExactlyInAnyOrder(
                    withJobId("partition-b-job", leftReference),
                    withJobId("partition-c-job", rightReference));
        }
    }

    @Nested
    @DisplayName("Limit compaction numbers to single lambda invocation")
    class CompactionJobLimitationsForInvocation {
        @Test
        void shouldCreateJobsLimitedDownToCreationLimitWhenTheCompactionJobsExceedTheValue() throws Exception {
            // Given normal compaction we set a limit for the creation to be less than the entries present
            Random rand = new Random(0);

            tableProperties.set(COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName());
            tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "3");
            instanceProperties.set(COMPACTION_JOB_CREATION_LIMIT, "2");
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "bbb")
                    .splitToNewChildren("L", "LL", "LR", "aaa")
                    .buildList());

            FileReference fileReference1 = fileFactory().partitionFile("R", "file1", 200L);
            FileReference fileReference2 = fileFactory().partitionFile("LL", "file2", 200L);
            FileReference fileReference3 = fileFactory().partitionFile("LR", "file3", 200L);
            stateStore.addFiles(List.of(fileReference1, fileReference2, fileReference3));

            // When we force create jobs
            createJobWithForceAllFiles(fixJobIds("partition-R-job", "partition-LL-job", "partition-LR-job"), rand);

            // Then
            assertThat(jobs).containsExactly(
                    compactionFactory().createCompactionJob("partition-R-job", List.of(fileReference1), "R"),
                    compactionFactory().createCompactionJob("partition-LR-job", List.of(fileReference3), "LR"));
        }
    }

    @Nested
    @DisplayName("Compact all files")
    class CompactAllFiles {

        @Test
        void shouldCreateJobsWhenStrategyDoesNotCreateJobsForWholeFilesWhenCompactingAllFiles() throws Exception {
            // Given we use the BasicCompactionStrategy with a batch size of 3
            tableProperties.set(COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName());
            tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "3");
            stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            // And we have 2 active whole files in the state store (which the BasicCompactionStrategy will skip
            // as it does not create jobs with fewer files than the batch size)
            FileReference fileReference1 = fileFactory().rootFile("file1", 200L);
            FileReference fileReference2 = fileFactory().rootFile("file2", 200L);
            stateStore.addFiles(List.of(fileReference1, fileReference2));

            // When we force create jobs
            createJobWithForceAllFiles(fixJobIds("test-job"));

            // Then a compaction job will be created for the files skipped by the BasicCompactionStrategy
            assertThat(jobs).containsExactly(
                    compactionFactory().createCompactionJob("test-job", List.of(fileReference1, fileReference2), "root"));
        }

        @Test
        void shouldCreateJobsWhenStrategyDoesNotCreateJobsForSplitFilesWhenCompactingAllFiles() throws Exception {
            // Given we use the BasicCompactionStrategy with a batch size of 3
            tableProperties.set(COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName());
            tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "3");
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "aaa")
                    .buildList());
            // And we have 1 active file that has been split in the state store (which the BasicCompactionStrategy
            // will skip as it does not create jobs with fewer files than the batch size)
            FileReference rootFile = fileFactory().rootFile("file1", 2L);
            FileReference fileReference1 = referenceForChildPartition(rootFile, "L");
            stateStore.addFile(fileReference1);

            // When we force create jobs
            createJobWithForceAllFiles(fixJobIds("test-job"));

            // Then a compaction job will be created for the files skipped by the BasicCompactionStrategy
            assertThat(jobs).containsExactly(
                    compactionFactory().createCompactionJob("test-job", List.of(fileReference1), "L"));
        }
    }

    @Nested
    @DisplayName("Assign input files to job in state store")
    class AssignInputFiles {

        @Test
        void shouldAssignMultipleFilesToCompactionJob() throws Exception {
            // Given we have files for compaction
            stateStore.initialise(new PartitionsBuilder(schema)
                    .singlePartition("1")
                    .buildList());

            FileReference fileOne = fileFactory().rootFile("fileOne", 1L);
            FileReference fileTwo = fileFactory().rootFile("fileTwo", 2L);
            stateStore.addFiles(List.of(fileOne, fileTwo));

            // When
            createJobWithForceAllFiles(fixJobIds("test-job"));

            // Then
            CompactionJob expectedJob = compactionFactory().createCompactionJob("test-job", List.of(fileOne, fileTwo), "1");
            assertThat(jobs).containsExactly(expectedJob);
            assertThat(stateStore.getFileReferences())
                    .containsExactly(
                            withJobId("test-job", fileOne),
                            withJobId("test-job", fileTwo));
        }

        @Test
        void shouldAssignFilesToMultipleCompactionJobs() throws Exception {
            // Given we have files for compaction
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "aaa")
                    .buildList());

            FileReference leftFile = fileFactory().partitionFile("L", "leftFile", 1L);
            FileReference rightFile = fileFactory().partitionFile("R", "rightFile", 2L);
            stateStore.addFiles(List.of(leftFile, rightFile));

            // When
            createJobWithForceAllFiles(fixJobIds("left-job", "right-job"));

            // Then
            CompactionJob leftJob = compactionFactory().createCompactionJob("left-job", List.of(leftFile), "L");
            CompactionJob rightJob = compactionFactory().createCompactionJob("right-job", List.of(rightFile), "R");
            assertThat(jobs).containsExactly(leftJob, rightJob);
            assertThat(stateStore.getFileReferences())
                    .containsExactly(
                            withJobId("left-job", leftFile),
                            withJobId("right-job", rightFile));
        }
    }

    @Nested
    @DisplayName("Save job created update in status store")
    class SaveJobCreatedStatusUpdate {

        @Test
        void shouldSaveJobCreatedUpdatesForMultipleJobsWhenForceCreated() throws Exception {
            // Given some partitions with files to be compacted
            tableProperties.setNumber(COMPACTION_JOB_SEND_BATCH_SIZE, 1);
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "aaa")
                    .buildList());
            FileReference leftFile = fileFactory().partitionFile("L", "leftFile", 1L);
            FileReference rightFile = fileFactory().partitionFile("R", "rightFile", 2L);
            stateStore.addFiles(List.of(leftFile, rightFile));

            Instant createdTime1 = Instant.parse("2024-09-06T10:11:00Z");
            Instant createdTime2 = Instant.parse("2024-09-06T10:11:01Z");
            jobStatusStore.setTimeSupplier(Stream.of(createdTime1, createdTime2).iterator()::next);

            // When we create compaction jobs
            createJobWithForceAllFiles(fixJobIds("left-job", "right-job"));

            // Then the jobs are reported as created in the status store
            CompactionJob leftJob = compactionFactory().createCompactionJob("left-job", List.of(leftFile), "L");
            CompactionJob rightJob = compactionFactory().createCompactionJob("right-job", List.of(rightFile), "R");
            assertThat(jobs).containsExactly(leftJob, rightJob);
            assertThat(jobStatusStore.getAllJobs(tableProperties.get(TABLE_ID))).containsExactly(
                    jobCreated(rightJob, createdTime2),
                    jobCreated(leftJob, createdTime1));
        }

        @Test
        void shouldSaveJobCreatedUpdatesForMultipleJobsWhenCreatedByStrategy() throws Exception {
            // Given some partitions with files to be compacted
            tableProperties.setNumber(COMPACTION_JOB_SEND_BATCH_SIZE, 1);
            tableProperties.set(COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName());
            tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "1");
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "aaa")
                    .buildList());
            FileReference leftFile = fileFactory().partitionFile("L", "leftFile", 1L);
            FileReference rightFile = fileFactory().partitionFile("R", "rightFile", 2L);
            stateStore.addFiles(List.of(leftFile, rightFile));
            jobStatusStore.fixUpdateTime(DEFAULT_UPDATE_TIME);

            Instant createdTime1 = Instant.parse("2024-09-06T10:11:00Z");
            Instant createdTime2 = Instant.parse("2024-09-06T10:11:01Z");
            jobStatusStore.setTimeSupplier(Stream.of(createdTime1, createdTime2).iterator()::next);

            // When we create compaction jobs
            createJobsWithStrategy(fixJobIds("left-job", "right-job"));

            // Then the jobs are reported as created in the status store
            CompactionJob leftJob = compactionFactory().createCompactionJob("left-job", List.of(leftFile), "L");
            CompactionJob rightJob = compactionFactory().createCompactionJob("right-job", List.of(rightFile), "R");
            assertThat(jobs).containsExactly(leftJob, rightJob);
            assertThat(jobStatusStore.getAllJobs(tableProperties.get(TABLE_ID))).containsExactly(
                    jobCreated(rightJob, createdTime2),
                    jobCreated(leftJob, createdTime1));
        }

        @Test
        void shouldNotSaveFilesAssignedUpdateWithAsynchronousCommit() throws Exception {
            // Given
            tableProperties.setNumber(COMPACTION_JOB_SEND_BATCH_SIZE, 1);
            tableProperties.set(COMPACTION_JOB_ID_ASSIGNMENT_COMMIT_ASYNC, "true");
            stateStore.initialise(new PartitionsBuilder(schema)
                    .singlePartition("root")
                    .buildList());
            FileReference file = fileFactory().rootFile("test.parquet", 100L);
            stateStore.addFiles(List.of(file));

            Instant createdTime = Instant.parse("2024-09-06T10:11:00Z");
            jobStatusStore.setTimeSupplier(Stream.of(createdTime).iterator()::next);

            // When
            createJobWithForceAllFiles(fixJobIds("test-job"));

            // Then
            CompactionJob job = compactionFactory().createCompactionJob("test-job", List.of(file), "root");
            assertThat(jobs).containsExactly(job);
            assertThat(jobStatusStore.getAllJobs(tableProperties.get(TABLE_ID))).isEmpty();
            assertThat(jobIdAssignmentCommitRequests).containsExactly(
                    CompactionJobIdAssignmentCommitRequest.tableRequests(tableProperties.get(TABLE_ID),
                            List.of(assignJobOnPartitionToFiles("test-job", "root", List.of("test.parquet")))));
        }
    }

    @Nested
    @DisplayName("Send jobs in batches")
    class SendInBatches {

        @Test
        void shouldSendMultipleBatches() throws Exception {
            // Given
            instanceProperties.set(DATA_BUCKET, "test-bucket");
            tableProperties.set(TABLE_ID, "test-table");
            tableProperties.setNumber(COMPACTION_JOB_SEND_BATCH_SIZE, 2);
            stateStore.initialise(new PartitionsBuilder(schema)
                    .singlePartition("root")
                    .splitToNewChildren("root", "L", "R", "m")
                    .splitToNewChildren("R", "RL", "RR", "s")
                    .buildList());
            FileReference file1 = fileFactory().partitionFile("L", 100L);
            FileReference file2 = fileFactory().partitionFile("RL", 200L);
            FileReference file3 = fileFactory().partitionFile("RR", 300L);
            stateStore.addFiles(List.of(file1, file2, file3));

            // When
            createJobWithForceAllFiles(
                    fixJobIds("job-1", "job-2", "job-3"),
                    fixBatchIds("batch-1", "batch-2"),
                    timePassesAMinuteAtATimeFrom(Instant.parse("2024-11-25T11:00:00Z")));

            // Then
            CompactionJob job1 = compactionFactory().createCompactionJob("job-1", List.of(file1), "L");
            CompactionJob job2 = compactionFactory().createCompactionJob("job-2", List.of(file2), "RL");
            CompactionJob job3 = compactionFactory().createCompactionJob("job-3", List.of(file3), "RR");
            CompactionJobDispatchRequest batch1 = batchRequest("batch-1", Instant.parse("2024-11-25T11:00:00Z"));
            CompactionJobDispatchRequest batch2 = batchRequest("batch-2", Instant.parse("2024-11-25T11:01:00Z"));
            assertThat(jobs).containsExactly(job1, job2, job3);
            assertThat(pendingQueue).containsExactly(batch1, batch2);
            assertThat(bucketAndKeyToJobs).isEqualTo(Map.of(
                    "test-bucket/test-table/compactions/batch-1.json", List.of(job1, job2),
                    "test-bucket/test-table/compactions/batch-2.json", List.of(job3)));
        }
    }

    private FileReferenceFactory fileFactory() {
        return FileReferenceFactory.fromUpdatedAt(stateStore, DEFAULT_UPDATE_TIME);
    }

    private CompactionJobFactory compactionFactory() {
        return new CompactionJobFactory(instanceProperties, tableProperties);
    }

    private void createJobsWithStrategy(GenerateJobId generateJobId) throws Exception {
        jobCreator(generateJobId, GenerateBatchId.random(), new Random(), timePassesAMinuteAtATime())
                .createJobsWithStrategy(tableProperties);
    }

    private void createJobWithForceAllFiles(GenerateJobId generateJobId, Random random) throws Exception {
        jobCreator(generateJobId, GenerateBatchId.random(), random, timePassesAMinuteAtATime())
                .createJobWithForceAllFiles(tableProperties);
    }

    private void createJobWithForceAllFiles(GenerateJobId generateJobId) throws Exception {
        jobCreator(generateJobId, GenerateBatchId.random(), new Random(), timePassesAMinuteAtATime())
                .createJobWithForceAllFiles(tableProperties);
    }

    private void createJobWithForceAllFiles(GenerateJobId generateJobId, GenerateBatchId generateBatchId, Supplier<Instant> timeSupplier) throws Exception {
        jobCreator(generateJobId, generateBatchId, new Random(), timeSupplier)
                .createJobWithForceAllFiles(tableProperties);
    }

    private CreateCompactionJobs jobCreator(
            GenerateJobId generateJobId, GenerateBatchId generateBatchId, Random random, Supplier<Instant> timeSupplier) throws Exception {
        return new CreateCompactionJobs(
                ObjectFactory.noUserJars(), instanceProperties,
                new FixedStateStoreProvider(tableProperties, stateStore),
                jobStatusStore, createBatchWriter(), pendingQueue::add, jobIdAssignmentCommitRequests::add,
                generateJobId, generateBatchId, random, timeSupplier);
    }

    private CompactionJobDispatchRequest batchRequest(String batchId, Instant createTime) {
        return CompactionJobDispatchRequest.forTableWithBatchIdAtTime(tableProperties, batchId, createTime);
    }

    private BatchJobsWriter createBatchWriter() {
        return (bucketName, key, batch) -> {
            jobs.addAll(batch);
            bucketAndKeyToJobs.put(bucketName + "/" + key, batch);
        };
    }

    private GenerateJobId fixJobIds(String... jobIds) {
        return fixIds(jobIds)::get;
    }

    private GenerateBatchId fixBatchIds(String... batchIds) {
        return fixIds(batchIds)::get;
    }

    private Supplier<Instant> timePassesAMinuteAtATime() {
        return timePassesAMinuteAtATimeFrom(Instant.parse("2024-11-25T10:50:00Z"));
    }

    private TableProperties createTable() {
        return CreateJobsTestUtils.createTableProperties(schema, instanceProperties);
    }

    private StateStore createStateStore(TableProperties table) {
        StateStore stateStore = inMemoryStateStoreUninitialised(schema);
        stateStore.fixFileUpdateTime(DEFAULT_UPDATE_TIME);
        return stateStore;
    }
}
