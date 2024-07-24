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
package sleeper.compaction.job.creation;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.compaction.job.creation.CreateCompactionJobs.Mode;
import sleeper.compaction.strategy.impl.BasicCompactionStrategy;
import sleeper.compaction.strategy.impl.SizeRatioCompactionStrategy;
import sleeper.compaction.testutils.InMemoryCompactionJobStatusStore;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.FixedStateStoreProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LIMIT;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_JOB_SEND_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.core.statestore.FileReferenceTestData.splitFile;
import static sleeper.core.statestore.FileReferenceTestData.withJobId;
import static sleeper.core.statestore.SplitFileReference.referenceForChildPartition;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreUninitialised;

public class CreateCompactionJobsTest {

    private final InstanceProperties instanceProperties = CreateJobsTestUtils.createInstanceProperties();
    private final Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
    private final InMemoryCompactionJobStatusStore jobStatusStore = new InMemoryCompactionJobStatusStore();
    private final TableProperties tableProperties = createTable();
    private final StateStore stateStore = createStateStore(tableProperties);
    private final List<CompactionJob> jobs = new ArrayList<>();

    @Nested
    @DisplayName("Compact files using strategy")
    class CompactFilesByStrategy {

        @Test
        public void shouldCompactAllFilesInSinglePartition() throws Exception {
            // Given
            tableProperties.set(COMPACTION_STRATEGY_CLASS, SizeRatioCompactionStrategy.class.getName());
            stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(stateStore, DEFAULT_UPDATE_TIME);
            FileReference fileReference1 = factory.rootFile("file1", 200L);
            FileReference fileReference2 = factory.rootFile("file2", 200L);
            FileReference fileReference3 = factory.rootFile("file3", 200L);
            FileReference fileReference4 = factory.rootFile("file4", 200L);
            List<FileReference> fileReferences = List.of(fileReference1, fileReference2, fileReference3, fileReference4);
            stateStore.addFiles(fileReferences);

            // When
            createJobs(Mode.STRATEGY, fixJobIds("test-job"));

            // Then
            CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
            assertThat(jobs).containsExactly(
                    jobFactory.createCompactionJob("test-job", fileReferences, "root"));
        }

        @Test
        public void shouldCompactFilesInDifferentPartitions() throws Exception {
            // Given
            tableProperties.set(COMPACTION_STRATEGY_CLASS, SizeRatioCompactionStrategy.class.getName());
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("A")
                    .splitToNewChildren("A", "B", "C", "ddd")
                    .buildList());
            FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(stateStore, DEFAULT_UPDATE_TIME);
            FileReference fileReference1 = factory.partitionFile("B", "file1", 200L);
            FileReference fileReference2 = factory.partitionFile("B", "file2", 200L);
            FileReference fileReference3 = factory.partitionFile("C", "file3", 200L);
            FileReference fileReference4 = factory.partitionFile("C", "file4", 200L);
            stateStore.addFiles(List.of(fileReference1, fileReference2, fileReference3, fileReference4));

            // When
            createJobs(Mode.STRATEGY, fixJobIds("partition-b-job", "partition-c-job"));

            // Then
            CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
            assertThat(jobs).containsExactly(
                    jobFactory.createCompactionJob("partition-b-job", List.of(fileReference1, fileReference2), "B"),
                    jobFactory.createCompactionJob("partition-c-job", List.of(fileReference3, fileReference4), "C"));
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
            FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(stateStore, DEFAULT_UPDATE_TIME);
            FileReference fileReference1 = factory.partitionFile("A", "file1", 200L);
            FileReference fileReference2 = factory.partitionFile("A", "file2", 200L);
            stateStore.addFiles(List.of(fileReference1, fileReference2));

            // When
            createJobs(Mode.STRATEGY, fixJobIds("partition-b-job", "partition-c-job"));

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
            FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(stateStore, DEFAULT_UPDATE_TIME);
            FileReference fileReference1 = factory.partitionFile("A", "file1", 200L);
            FileReference fileReference2 = factory.partitionFile("A", "file2", 200L);
            stateStore.addFiles(List.of(fileReference1, fileReference2));

            // When
            createJobs(Mode.STRATEGY, fixJobIds("partition-b-job", "partition-c-job"));

            // Then
            CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
            assertThat(jobs).containsExactly(
                    jobFactory.createCompactionJobWithFilenames("partition-b-job", List.of("file1", "file2"), "B"),
                    jobFactory.createCompactionJobWithFilenames("partition-c-job", List.of("file1", "file2"), "C"));
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
            FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(stateStore, DEFAULT_UPDATE_TIME);
            FileReference fileReference = factory.partitionFile("A", "file", 200L);
            FileReference leftReference = splitFile(fileReference, "B");
            FileReference rightReference = splitFile(fileReference, "C");
            stateStore.addFiles(List.of(leftReference, rightReference));

            // When
            createJobs(Mode.STRATEGY, fixJobIds("partition-b-job", "partition-c-job"));

            // Then
            CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
            assertThat(jobs).containsExactly(
                    jobFactory.createCompactionJobWithFilenames("partition-b-job", List.of("file"), "B"),
                    jobFactory.createCompactionJobWithFilenames("partition-c-job", List.of("file"), "C"));
            assertThat(stateStore.getFileReferences()).containsExactlyInAnyOrder(
                    withJobId("partition-b-job", leftReference),
                    withJobId("partition-c-job", rightReference));
        }
    }

    @Nested
    @DisplayName("Limit compaction numbers to single lambda invocation")
    class CompactionJobLimitationsForInvocation {
        @Test
        void shouldCreateJobsLimitedDownToExecutionLimitWhenTheCompactionJobsExceedTheValue() throws Exception {
            // Given normal compaction we set a limit for the execution to be less than the entries present
            Random rand = new Random(0);

            tableProperties.set(COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName());
            tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "3");
            instanceProperties.set(COMPACTION_JOB_CREATION_LIMIT, "2");
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "bbb")
                    .splitToNewChildren("L", "LL", "LR", "aaa")
                    .buildList());

            FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(stateStore, DEFAULT_UPDATE_TIME);
            FileReference fileReference1 = factory.partitionFile("R", "file1", 200L);
            FileReference fileReference2 = factory.partitionFile("LL", "file2", 200L);
            FileReference fileReference3 = factory.partitionFile("LR", "file3", 200L);
            stateStore.addFiles(List.of(fileReference1, fileReference2, fileReference3));

            // When we force create jobs
            createJobs(Mode.FORCE_ALL_FILES_AFTER_STRATEGY, fixJobIds("partition-R-job", "partition-LL-job", "partition-LR-job"), rand);

            // Then
            CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
            assertThat(jobs).containsExactly(
                    jobFactory.createCompactionJob("partition-R-job", List.of(fileReference1), "R"),
                    jobFactory.createCompactionJob("partition-LR-job", List.of(fileReference3), "LR"));
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
            FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(stateStore, DEFAULT_UPDATE_TIME);
            // And we have 2 active whole files in the state store (which the BasicCompactionStrategy will skip
            // as it does not create jobs with fewer files than the batch size)
            FileReference fileReference1 = factory.rootFile("file1", 200L);
            FileReference fileReference2 = factory.rootFile("file2", 200L);
            stateStore.addFiles(List.of(fileReference1, fileReference2));

            // When we force create jobs
            createJobs(Mode.FORCE_ALL_FILES_AFTER_STRATEGY, fixJobIds("test-job"));

            // Then a compaction job will be created for the files skipped by the BasicCompactionStrategy
            CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
            assertThat(jobs).containsExactly(
                    jobFactory.createCompactionJob("test-job", List.of(fileReference1, fileReference2), "root"));
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
            FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(stateStore, DEFAULT_UPDATE_TIME);
            // And we have 1 active file that has been split in the state store (which the BasicCompactionStrategy
            // will skip as it does not create jobs with fewer files than the batch size)
            FileReference rootFile = factory.rootFile("file1", 2L);
            FileReference fileReference1 = referenceForChildPartition(rootFile, "L");
            stateStore.addFile(fileReference1);

            // When we force create jobs
            createJobs(Mode.FORCE_ALL_FILES_AFTER_STRATEGY, fixJobIds("test-job"));

            // Then a compaction job will be created for the files skipped by the BasicCompactionStrategy
            CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
            assertThat(jobs).containsExactly(
                    jobFactory.createCompactionJob("test-job", List.of(fileReference1), "L"));
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

            FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(stateStore, DEFAULT_UPDATE_TIME);
            FileReference fileOne = factory.rootFile("fileOne", 1L);
            FileReference fileTwo = factory.rootFile("fileTwo", 2L);
            stateStore.addFiles(List.of(fileOne, fileTwo));

            // When
            createJobs(Mode.FORCE_ALL_FILES_AFTER_STRATEGY, fixJobIds("test-job"));

            // Then
            CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
            assertThat(jobs).containsExactly(
                    jobFactory.createCompactionJob("test-job", List.of(fileOne, fileTwo), "1"));
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

            FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(stateStore, DEFAULT_UPDATE_TIME);
            FileReference leftFile = factory.partitionFile("L", "leftFile", 1L);
            FileReference rightFile = factory.partitionFile("R", "rightFile", 2L);
            stateStore.addFiles(List.of(leftFile, rightFile));

            // When
            createJobs(Mode.FORCE_ALL_FILES_AFTER_STRATEGY, fixJobIds("left-job", "right-job"));

            // Then
            CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
            assertThat(jobs).containsExactly(
                    jobFactory.createCompactionJob("left-job", List.of(leftFile), "L"),
                    jobFactory.createCompactionJob("right-job", List.of(rightFile), "R"));
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
            FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(stateStore, DEFAULT_UPDATE_TIME);
            FileReference leftFile = factory.partitionFile("L", "leftFile", 1L);
            FileReference rightFile = factory.partitionFile("R", "rightFile", 2L);
            stateStore.addFiles(List.of(leftFile, rightFile));
            jobStatusStore.fixUpdateTime(DEFAULT_UPDATE_TIME);

            // When we create compaction jobs
            createJobs(Mode.FORCE_ALL_FILES_AFTER_STRATEGY, fixJobIds("left-job", "right-job"));

            // Then the jobs are reported as created in the status store
            CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
            CompactionJob rightJob = jobFactory.createCompactionJob("right-job", List.of(rightFile), "R");
            CompactionJob leftJob = jobFactory.createCompactionJob("left-job", List.of(leftFile), "L");
            assertThat(jobs).containsExactly(leftJob, rightJob);
            assertThat(jobStatusStore.getAllJobs(tableProperties.get(TABLE_ID))).containsExactly(
                    jobCreated(rightJob, DEFAULT_UPDATE_TIME),
                    jobCreated(leftJob, DEFAULT_UPDATE_TIME));
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
            FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(stateStore, DEFAULT_UPDATE_TIME);
            FileReference leftFile = factory.partitionFile("L", "leftFile", 1L);
            FileReference rightFile = factory.partitionFile("R", "rightFile", 2L);
            stateStore.addFiles(List.of(leftFile, rightFile));
            jobStatusStore.fixUpdateTime(DEFAULT_UPDATE_TIME);

            // When we create compaction jobs
            createJobs(Mode.STRATEGY, fixJobIds("left-job", "right-job"));

            // Then the jobs are reported as created in the status store
            CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
            CompactionJob rightJob = jobFactory.createCompactionJob("right-job", List.of(rightFile), "R");
            CompactionJob leftJob = jobFactory.createCompactionJob("left-job", List.of(leftFile), "L");
            assertThat(jobs).containsExactly(leftJob, rightJob);
            assertThat(jobStatusStore.getAllJobs(tableProperties.get(TABLE_ID))).containsExactly(
                    jobCreated(rightJob, DEFAULT_UPDATE_TIME),
                    jobCreated(leftJob, DEFAULT_UPDATE_TIME));
        }
    }

    private void createJobs(CreateCompactionJobs.Mode mode, Supplier<String> jobIdSupplier, Random random) throws Exception {
        jobCreator(mode, jobIdSupplier, random).createJobs(tableProperties);
    }

    private void createJobs(CreateCompactionJobs.Mode mode, Supplier<String> jobIdSupplier) throws Exception {
        jobCreator(mode, jobIdSupplier, new Random()).createJobs(tableProperties);
    }

    private CreateCompactionJobs jobCreator(CreateCompactionJobs.Mode mode, Supplier<String> jobIdSupplier, Random random) throws Exception {
        return new CreateCompactionJobs(
                ObjectFactory.noUserJars(), instanceProperties,
                new FixedStateStoreProvider(tableProperties, stateStore),
                jobs::add, jobStatusStore, mode, jobIdSupplier, random);
    }

    private Supplier<String> fixJobIds(String... jobIds) {
        return List.of(jobIds).iterator()::next;
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
