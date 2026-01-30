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
package sleeper.compaction.core.job.creation;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.core.job.creation.CreateCompactionJobBatches.BatchJobsWriter;
import sleeper.compaction.core.job.creation.CreateCompactionJobBatches.GenerateBatchId;
import sleeper.compaction.core.job.creation.CreateCompactionJobs.GenerateJobId;
import sleeper.compaction.core.job.creation.strategy.impl.BasicCompactionStrategy;
import sleeper.compaction.core.job.creation.strategy.impl.SizeRatioCompactionStrategy;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequest;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.statestore.transactionlog.transaction.impl.AssignJobIdsTransaction;
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

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_CREATION_LIMIT;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_ID_ASSIGNMENT_COMMIT_ASYNC;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_SEND_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.core.statestore.FileReferenceTestData.splitFile;
import static sleeper.core.statestore.FileReferenceTestData.withJobId;
import static sleeper.core.statestore.SplitFileReference.referenceForChildPartition;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.core.testutils.SupplierTestHelper.fixIds;
import static sleeper.core.testutils.SupplierTestHelper.timePassesAMinuteAtATimeFrom;

public class CreateCompactionJobsTest {

    InstanceProperties instanceProperties = CreateJobsTestUtils.createInstanceProperties();
    Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
    TableProperties tableProperties = createTable();
    StateStore stateStore = createStateStore(tableProperties);

    List<CompactionJob> jobs = new ArrayList<>();
    Queue<CompactionJobDispatchRequest> pendingQueue = new LinkedList<>();
    Map<String, List<CompactionJob>> bucketAndKeyToJobs = new HashMap<>();
    List<StateStoreCommitRequest> jobIdAssignmentCommitRequests = new ArrayList<>();

    @Nested
    @DisplayName("Compact files using strategy")
    class CompactFilesByStrategy {

        @Test
        public void shouldCompactAllFilesInSinglePartition() throws Exception {
            // Given
            tableProperties.set(COMPACTION_STRATEGY_CLASS, SizeRatioCompactionStrategy.class.getName());
            update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            FileReference fileReference1 = fileFactory().rootFile("file1", 200L);
            FileReference fileReference2 = fileFactory().rootFile("file2", 200L);
            FileReference fileReference3 = fileFactory().rootFile("file3", 200L);
            FileReference fileReference4 = fileFactory().rootFile("file4", 200L);
            List<FileReference> fileReferences = List.of(fileReference1, fileReference2, fileReference3, fileReference4);
            update(stateStore).addFiles(fileReferences);

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
            update(stateStore).initialise(new PartitionsBuilder(schema)
                    .rootFirst("A")
                    .splitToNewChildren("A", "B", "C", "ddd")
                    .buildList());
            FileReference fileReference1 = fileFactory().partitionFile("B", "file1", 200L);
            FileReference fileReference2 = fileFactory().partitionFile("B", "file2", 200L);
            FileReference fileReference3 = fileFactory().partitionFile("C", "file3", 200L);
            FileReference fileReference4 = fileFactory().partitionFile("C", "file4", 200L);
            update(stateStore).addFiles(List.of(fileReference1, fileReference2, fileReference3, fileReference4));

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
            update(stateStore).initialise(new PartitionsBuilder(schema)
                    .rootFirst("A")
                    .splitToNewChildren("A", "B", "C", "ddd")
                    .splitToNewChildren("B", "B1", "B2", "aaa")
                    .splitToNewChildren("C", "C1", "C2", "fff")
                    .buildList());
            FileReference fileReference1 = fileFactory().partitionFile("A", "file1", 200L);
            FileReference fileReference2 = fileFactory().partitionFile("A", "file2", 200L);
            update(stateStore).addFiles(List.of(fileReference1, fileReference2));

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
            update(stateStore).initialise(new PartitionsBuilder(schema)
                    .rootFirst("A")
                    .splitToNewChildren("A", "B", "C", "ddd")
                    .buildList());
            FileReference fileReference1 = fileFactory().partitionFile("A", "file1", 200L);
            FileReference fileReference2 = fileFactory().partitionFile("A", "file2", 200L);
            update(stateStore).addFiles(List.of(fileReference1, fileReference2));

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
            update(stateStore).initialise(new PartitionsBuilder(schema)
                    .rootFirst("A")
                    .splitToNewChildren("A", "B", "C", "ddd")
                    .buildList());
            FileReference fileReference = fileFactory().partitionFile("A", "file", 200L);
            FileReference leftReference = splitFile(fileReference, "B");
            FileReference rightReference = splitFile(fileReference, "C");
            update(stateStore).addFiles(List.of(leftReference, rightReference));

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
            tableProperties.set(COMPACTION_JOB_CREATION_LIMIT, "2");
            update(stateStore).initialise(new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "bbb")
                    .splitToNewChildren("L", "LL", "LR", "aaa")
                    .buildList());

            FileReference fileReference1 = fileFactory().partitionFile("R", "file1", 200L);
            FileReference fileReference2 = fileFactory().partitionFile("LL", "file2", 200L);
            FileReference fileReference3 = fileFactory().partitionFile("LR", "file3", 200L);
            update(stateStore).addFiles(List.of(fileReference1, fileReference2, fileReference3));

            // When we force create jobs
            createJobWithForceAllFiles(GenerateJobId.random(), rand);

            // Then
            CompactionJob job1 = compactionFactory().createCompactionJob("partition-R-job", List.of(fileReference1), "R");
            CompactionJob job2 = compactionFactory().createCompactionJob("partition-LL-job", List.of(fileReference2), "LL");
            CompactionJob job3 = compactionFactory().createCompactionJob("partition-LR-job", List.of(fileReference3), "LR");
            assertThat(jobs).hasSize(2)
                    // Output file is derived from job ID, and we don't want to be sensitive to order in which jobs are created.
                    // We still assert on the input files and partition ID.
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("jobId", "outputFile")
                    .isSubsetOf(job1, job2, job3);
        }

        @Test
        void shouldCreateJobsLimitedDownToCreationLimitWhenTheCompactionJobsExceedTheValueIncludingRunningJobs() throws Exception {
            // Given normal compaction we set a limit for the creation to be less than the entries present
            Random rand = new Random(0);

            tableProperties.set(COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName());
            tableProperties.set(COMPACTION_JOB_CREATION_LIMIT, "3");
            update(stateStore).initialise(new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "bbb")
                    .splitToNewChildren("L", "LL", "LR", "aaa")
                    .buildList());

            FileReference fileReference1 = fileFactory().partitionFile("R", "file1", 200L);
            FileReference fileReference2 = fileFactory().partitionFile("LL", "file2", 200L);
            FileReference fileReference3 = fileFactory().partitionFile("LR", "file3", 200L);
            FileReference fileReference4 = fileFactory().partitionFile("L", "file4", 200L);
            update(stateStore).addFiles(List.of(fileReference1, fileReference2, fileReference3, fileReference4));
            update(stateStore).assignJobId("job-3", List.of(fileReference3));
            update(stateStore).assignJobId("job-4", List.of(fileReference4));

            // When we force create jobs
            createJobWithForceAllFiles(GenerateJobId.random(), rand);

            // Then
            CompactionJob job1 = compactionFactory().createCompactionJob("partition-R-job", List.of(fileReference1), "R");
            CompactionJob job2 = compactionFactory().createCompactionJob("partition-LL-job", List.of(fileReference2), "LL");
            assertThat(jobs).hasSize(1)
                    // Output file is derived from job ID, and we don't want to be sensitive to order in which jobs are created.
                    // We still assert on the input files and partition ID.
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("jobId", "outputFile")
                    .isSubsetOf(job1, job2);
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
            update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            // And we have 2 whole files in the state store (which the BasicCompactionStrategy will skip
            // as it does not create jobs with fewer files than the batch size)
            FileReference fileReference1 = fileFactory().rootFile("file1", 200L);
            FileReference fileReference2 = fileFactory().rootFile("file2", 200L);
            update(stateStore).addFiles(List.of(fileReference1, fileReference2));

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
            update(stateStore).initialise(new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "aaa")
                    .buildList());
            // And we have 1 file that has been split in the state store (which the BasicCompactionStrategy
            // will skip as it does not create jobs with fewer files than the batch size)
            FileReference rootFile = fileFactory().rootFile("file1", 2L);
            FileReference fileReference1 = referenceForChildPartition(rootFile, "L");
            update(stateStore).addFile(fileReference1);

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
            update(stateStore).initialise(new PartitionsBuilder(schema)
                    .singlePartition("1")
                    .buildList());

            FileReference fileOne = fileFactory().rootFile("fileOne", 1L);
            FileReference fileTwo = fileFactory().rootFile("fileTwo", 2L);
            update(stateStore).addFiles(List.of(fileOne, fileTwo));

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
            update(stateStore).initialise(new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "aaa")
                    .buildList());

            FileReference leftFile = fileFactory().partitionFile("L", "leftFile", 1L);
            FileReference rightFile = fileFactory().partitionFile("R", "rightFile", 2L);
            update(stateStore).addFiles(List.of(leftFile, rightFile));

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
    @DisplayName("Send jobs in batches")
    class SendInBatches {

        @Test
        void shouldSendMultipleBatches() throws Exception {
            // Given
            instanceProperties.set(DATA_BUCKET, "test-bucket");
            tableProperties.set(TABLE_ID, "test-table");
            tableProperties.setNumber(COMPACTION_JOB_SEND_BATCH_SIZE, 2);
            update(stateStore).initialise(new PartitionsBuilder(schema)
                    .singlePartition("root")
                    .splitToNewChildren("root", "L", "R", "m")
                    .splitToNewChildren("R", "RL", "RR", "s")
                    .buildList());
            FileReference file1 = fileFactory().partitionFile("L", 100L);
            FileReference file2 = fileFactory().partitionFile("RL", 200L);
            FileReference file3 = fileFactory().partitionFile("RR", 300L);
            update(stateStore).addFiles(List.of(file1, file2, file3));

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

    @Nested
    @DisplayName("Save file assignment asynchronously")
    class SaveFileAssignmentAsync {

        @Test
        void shouldSendAsynchronousCommitOfInputFileAssignment() throws Exception {
            // Given
            tableProperties.setNumber(COMPACTION_JOB_SEND_BATCH_SIZE, 1);
            tableProperties.set(COMPACTION_JOB_ID_ASSIGNMENT_COMMIT_ASYNC, "true");
            update(stateStore).initialise(new PartitionsBuilder(schema)
                    .singlePartition("root")
                    .buildList());
            FileReference file = fileFactory().rootFile("test.parquet", 100L);
            update(stateStore).addFiles(List.of(file));

            // When
            createJobWithForceAllFiles(fixJobIds("test-job"));

            // Then
            CompactionJob job = compactionFactory().createCompactionJob("test-job", List.of(file), "root");
            assertThat(jobs).containsExactly(job);
            assertThat(jobIdAssignmentCommitRequests).containsExactly(
                    StateStoreCommitRequest.create(tableProperties.get(TABLE_ID), new AssignJobIdsTransaction(
                            List.of(assignJobOnPartitionToFiles("test-job", "root", List.of("test.parquet"))))));
        }

        @Test
        void shouldSendAsynchronousCommitOfInputFileAssignmentBatches() throws Exception {
            // Given
            tableProperties.setNumber(COMPACTION_JOB_SEND_BATCH_SIZE, 2);
            tableProperties.set(COMPACTION_JOB_ID_ASSIGNMENT_COMMIT_ASYNC, "true");
            update(stateStore).initialise(new PartitionsBuilder(schema)
                    .singlePartition("root")
                    .splitToNewChildren("root", "L", "R", "j")
                    .splitToNewChildren("R", "RL", "RR", "t")
                    .buildList());
            FileReference lFile = fileFactory().partitionFile("L", 123L);
            FileReference rlFile = fileFactory().partitionFile("RL", 456L);
            FileReference rrFile = fileFactory().partitionFile("RR", 789L);
            update(stateStore).addFiles(List.of(lFile, rlFile, rrFile));

            // When
            createJobWithForceAllFiles(fixJobIds("job-1", "job-2", "job-3"));

            // Then
            CompactionJob job1 = compactionFactory().createCompactionJob("job-1", List.of(lFile), "L");
            CompactionJob job2 = compactionFactory().createCompactionJob("job-2", List.of(rlFile), "RL");
            CompactionJob job3 = compactionFactory().createCompactionJob("job-3", List.of(rrFile), "RR");
            assertThat(jobs).containsExactly(job1, job2, job3);
            assertThat(jobIdAssignmentCommitRequests).containsExactly(
                    StateStoreCommitRequest.create(tableProperties.get(TABLE_ID), new AssignJobIdsTransaction(
                            List.of(job1.createAssignJobIdRequest(), job2.createAssignJobIdRequest()))),
                    StateStoreCommitRequest.create(tableProperties.get(TABLE_ID), new AssignJobIdsTransaction(
                            List.of(job3.createAssignJobIdRequest()))));
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
                instanceProperties, FixedStateStoreProvider.singleTable(tableProperties, stateStore),
                new CreateCompactionJobBatches(instanceProperties, createBatchWriter(), pendingQueue::add, jobIdAssignmentCommitRequests::add, generateBatchId, timeSupplier),
                ObjectFactory.noUserJars(), generateJobId, random);
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
        StateStore stateStore = InMemoryTransactionLogStateStore.create(table, new InMemoryTransactionLogs());
        stateStore.fixFileUpdateTime(DEFAULT_UPDATE_TIME);
        return stateStore;
    }
}
