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
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.creation.CreateCompactionJobs.Mode;
import sleeper.compaction.strategy.impl.BasicCompactionStrategy;
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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.statestore.SplitFileReference.referenceForChildPartition;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreUninitialised;

public class CreateCompactionJobsTest {

    private static final Instant DEFAULT_UPDATE_TIME = Instant.parse("2024-02-13T11:19:00Z");
    private final InstanceProperties instanceProperties = CreateJobsTestUtils.createInstanceProperties();
    private final Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
    private final CompactionJobStatusStore jobStatusStore = new InMemoryCompactionJobStatusStore();
    private final TableProperties tableProperties = createTable();
    private final StateStore stateStore = createStateStore(tableProperties);
    private final List<CompactionJob> jobs = new ArrayList<>();

    @Nested
    @DisplayName("Compact files using strategy")
    class CompactFilesByStrategy {

        @Test
        public void shouldCompactAllFilesInSinglePartition() throws Exception {
            // Given
            stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(stateStore, DEFAULT_UPDATE_TIME);
            FileReference fileReference1 = factory.rootFile("file1", 200L);
            FileReference fileReference2 = factory.rootFile("file2", 200L);
            FileReference fileReference3 = factory.rootFile("file3", 200L);
            FileReference fileReference4 = factory.rootFile("file4", 200L);
            List<FileReference> fileReferences = List.of(fileReference1, fileReference2, fileReference3, fileReference4);
            stateStore.addFiles(fileReferences);

            // When
            createJobs(Mode.STRATEGY);

            // Then
            assertThat(jobs).singleElement().satisfies(job -> {
                assertThat(job).isEqualTo(CompactionJob.builder()
                        .jobId(job.getId())
                        .tableId(tableProperties.get(TABLE_ID))
                        .inputFiles(List.of("file1", "file2", "file3", "file4"))
                        .outputFile(job.getOutputFile())
                        .partitionId("root")
                        .build());
                assertThat(stateStore.getFileReferences())
                        .containsExactlyElementsOf(
                                withJobIds(fileReferences, job.getId()));
                verifyJobCreationReported(job);
            });
        }

        @Test
        public void shouldCompactFilesInDifferentPartitions() throws Exception {
            // Given
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
            createJobs(Mode.STRATEGY);

            // Then
            assertThat(jobs).satisfiesExactlyInAnyOrder(job -> {
                assertThat(job).isEqualTo(CompactionJob.builder()
                        .jobId(job.getId())
                        .tableId(tableProperties.get(TABLE_ID))
                        .inputFiles(List.of("file1", "file2"))
                        .outputFile(job.getOutputFile())
                        .partitionId("B")
                        .build());
                assertThat(stateStore.getFileReferences())
                        .contains(
                                withJobId(fileReference1, job.getId()),
                                withJobId(fileReference2, job.getId()));
                verifyJobCreationReported(job);
            }, job -> {
                assertThat(job).isEqualTo(CompactionJob.builder()
                        .jobId(job.getId())
                        .tableId(tableProperties.get(TABLE_ID))
                        .inputFiles(List.of("file3", "file4"))
                        .outputFile(job.getOutputFile())
                        .partitionId("C")
                        .build());
                assertThat(stateStore.getFileReferences())
                        .contains(
                                withJobId(fileReference3, job.getId()),
                                withJobId(fileReference4, job.getId()));
                verifyJobCreationReported(job);
            });
        }

        @Test
        public void shouldCreateCompactionJobAfterPreSplittingFiles() throws Exception {
            // Given
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("A")
                    .splitToNewChildren("A", "B", "C", "ddd")
                    .buildList());
            FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(stateStore, DEFAULT_UPDATE_TIME);
            FileReference fileReference1 = factory.partitionFile("A", "file1", 200L);
            FileReference fileReference2 = factory.partitionFile("A", "file2", 200L);
            stateStore.addFiles(List.of(fileReference1, fileReference2));

            // When
            createJobs(Mode.STRATEGY);

            // Then
            assertThat(jobs).satisfiesExactlyInAnyOrder(job -> {
                assertThat(job).isEqualTo(CompactionJob.builder()
                        .jobId(job.getId())
                        .tableId(tableProperties.get(TABLE_ID))
                        .inputFiles(List.of("file1", "file2"))
                        .outputFile(job.getOutputFile())
                        .partitionId("B")
                        .build());
                assertThat(stateStore.getFileReferences())
                        .contains(
                                withJobId(referenceForChildPartition(fileReference1, "B"), job.getId()),
                                withJobId(referenceForChildPartition(fileReference2, "B"), job.getId()));
                verifyJobCreationReported(job);
            }, job -> {
                assertThat(job).isEqualTo(CompactionJob.builder()
                        .jobId(job.getId())
                        .tableId(tableProperties.get(TABLE_ID))
                        .inputFiles(List.of("file1", "file2"))
                        .outputFile(job.getOutputFile())
                        .partitionId("C")
                        .build());
                assertThat(stateStore.getFileReferences())
                        .contains(
                                withJobId(referenceForChildPartition(fileReference1, "C"), job.getId()),
                                withJobId(referenceForChildPartition(fileReference2, "C"), job.getId()));
                verifyJobCreationReported(job);
            });
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
            FileReference leftReference = referenceForChildPartition(fileReference, "B");
            FileReference rightReference = referenceForChildPartition(fileReference, "C");
            stateStore.addFiles(List.of(leftReference, rightReference));

            // When
            createJobs(Mode.STRATEGY);

            // Then
            assertThat(jobs).satisfiesExactlyInAnyOrder(job -> {
                assertThat(job).isEqualTo(CompactionJob.builder()
                        .jobId(job.getId())
                        .tableId(tableProperties.get(TABLE_ID))
                        .inputFiles(List.of(leftReference.getFilename()))
                        .outputFile(job.getOutputFile())
                        .partitionId("B")
                        .build());
                assertThat(stateStore.getFileReferences())
                        .contains(
                                withJobId(leftReference, job.getId()));
                verifyJobCreationReported(job);
            }, job -> {
                assertThat(job).isEqualTo(CompactionJob.builder()
                        .jobId(job.getId())
                        .tableId(tableProperties.get(TABLE_ID))
                        .inputFiles(List.of(rightReference.getFilename()))
                        .outputFile(job.getOutputFile())
                        .partitionId("C")
                        .build());
                assertThat(stateStore.getFileReferences())
                        .contains(
                                withJobId(rightReference, job.getId()));
                verifyJobCreationReported(job);
            });
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
            createJobs(Mode.FORCE_ALL_FILES_AFTER_STRATEGY);

            // Then a compaction job will be created for the files skipped by the BasicCompactionStrategy
            assertThat(jobs).satisfiesExactly(job -> {
                assertThat(job).isEqualTo(CompactionJob.builder()
                        .jobId(job.getId())
                        .tableId(tableProperties.get(TABLE_ID))
                        .inputFiles(List.of("file1", "file2"))
                        .outputFile(job.getOutputFile())
                        .partitionId("root")
                        .build());
                assertThat(stateStore.getFileReferences())
                        .containsExactly(
                                withJobId(fileReference1, job.getId()),
                                withJobId(fileReference2, job.getId()));
                verifyJobCreationReported(job);
            });
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
            createJobs(Mode.FORCE_ALL_FILES_AFTER_STRATEGY);

            // Then a compaction job will be created for the files skipped by the BasicCompactionStrategy
            assertThat(jobs).satisfiesExactly(job -> {
                assertThat(job).isEqualTo(CompactionJob.builder()
                        .jobId(job.getId())
                        .tableId(tableProperties.get(TABLE_ID))
                        .inputFiles(List.of("file1"))
                        .outputFile(job.getOutputFile())
                        .partitionId("L")
                        .build());
                assertThat(stateStore.getFileReferences())
                        .containsExactly(
                                withJobId(fileReference1, job.getId()));
                verifyJobCreationReported(job);
            });
        }
    }

    private void verifyJobCreationReported(CompactionJob job) {
        assertThat(jobStatusStore.getJob(job.getId()).orElseThrow())
                .usingRecursiveComparison().ignoringFields("createdStatus.updateTime")
                .isEqualTo(jobCreated(job, Instant.MAX));
    }

    private List<FileReference> withJobIds(List<FileReference> fileReferences, String jobId) {
        return fileReferences.stream()
                .map(reference -> withJobId(reference, jobId))
                .collect(Collectors.toList());
    }

    private FileReference withJobId(FileReference fileReference, String jobId) {
        return fileReference.toBuilder().jobId(jobId).lastStateStoreUpdateTime(DEFAULT_UPDATE_TIME).build();
    }

    private void createJobs(CreateCompactionJobs.Mode mode) throws Exception {
        jobCreator(mode).createJobs(tableProperties);
    }

    private CreateCompactionJobs jobCreator(CreateCompactionJobs.Mode mode) throws Exception {
        return new CreateCompactionJobs(
                ObjectFactory.noUserJars(), instanceProperties,
                new FixedStateStoreProvider(tableProperties, stateStore),
                jobs::add, jobStatusStore, mode);
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
