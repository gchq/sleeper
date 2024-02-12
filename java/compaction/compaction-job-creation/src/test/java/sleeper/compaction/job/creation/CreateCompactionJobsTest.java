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

import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.strategy.impl.BasicCompactionStrategy;
import sleeper.compaction.testutils.InMemoryCompactionJobStatusStore;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
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
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithNoPartitions;

public class CreateCompactionJobsTest {

    private final InstanceProperties instanceProperties = CreateJobsTestUtils.createInstanceProperties();
    private final Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
    private final TableProperties tableProperties = CreateJobsTestUtils.createTableProperties(schema, instanceProperties);
    private final StateStore stateStore = inMemoryStateStoreWithNoPartitions();
    private final CompactionJobStatusStore jobStatusStore = new InMemoryCompactionJobStatusStore();

    @Test
    public void shouldCompactAllFilesInSinglePartition() throws Exception {
        // Given
        setPartitions(new PartitionsBuilder(schema).singlePartition("root").buildList());
        FileReferenceFactory fileReferenceFactory = fileReferenceFactory();
        FileReference fileReference1 = fileReferenceFactory.rootFile("file1", 200L);
        FileReference fileReference2 = fileReferenceFactory.rootFile("file2", 200L);
        FileReference fileReference3 = fileReferenceFactory.rootFile("file3", 200L);
        FileReference fileReference4 = fileReferenceFactory.rootFile("file4", 200L);
        List<FileReference> fileReferences = List.of(fileReference1, fileReference2, fileReference3, fileReference4);
        setFileReferences(fileReferences);

        // When
        List<CompactionJob> jobs = createJobs();

        // Then
        assertThat(jobs).singleElement().satisfies(job -> {
            assertThat(job).isEqualTo(CompactionJob.builder()
                    .jobId(job.getId())
                    .tableId(tableProperties.get(TABLE_ID))
                    .inputFiles(List.of("file1", "file2", "file3", "file4"))
                    .outputFile(job.getOutputFile())
                    .partitionId("root")
                    .build());
            verifySetJobForFilesInStateStore(job.getId(), fileReferences);
            verifyJobCreationReported(job);
        });
    }

    @Test
    public void shouldCompactFilesInDifferentPartitions() throws Exception {
        // Given
        List<Partition> partitions = new PartitionsBuilder(schema)
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "ddd")
                .buildList();
        setPartitions(partitions);
        FileReferenceFactory fileReferenceFactory = fileReferenceFactory();
        FileReference fileReference1 = fileReferenceFactory.partitionFile("B", "file1", 200L);
        FileReference fileReference2 = fileReferenceFactory.partitionFile("B", "file2", 200L);
        FileReference fileReference3 = fileReferenceFactory.partitionFile("C", "file3", 200L);
        FileReference fileReference4 = fileReferenceFactory.partitionFile("C", "file4", 200L);
        setFileReferences(List.of(fileReference1, fileReference2, fileReference3, fileReference4));

        // When
        List<CompactionJob> jobs = createJobs();

        // Then
        assertThat(jobs).satisfiesExactlyInAnyOrder(job -> {
            assertThat(job).isEqualTo(CompactionJob.builder()
                    .jobId(job.getId())
                    .tableId(tableProperties.get(TABLE_ID))
                    .inputFiles(List.of("file1", "file2"))
                    .outputFile(job.getOutputFile())
                    .partitionId("B")
                    .build());
            verifySetJobForFilesInStateStore(job.getId(), List.of(fileReference1, fileReference2));
            verifyJobCreationReported(job);
        }, job -> {
            assertThat(job).isEqualTo(CompactionJob.builder()
                    .jobId(job.getId())
                    .tableId(tableProperties.get(TABLE_ID))
                    .inputFiles(List.of("file3", "file4"))
                    .outputFile(job.getOutputFile())
                    .partitionId("C")
                    .build());
            verifySetJobForFilesInStateStore(job.getId(), List.of(fileReference3, fileReference4));
            verifyJobCreationReported(job);
        });
    }

    @Test
    public void shouldCreateCompactionJobAfterPreSplittingFiles() throws Exception {
        // Given
        List<Partition> partitions = new PartitionsBuilder(schema)
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "ddd")
                .buildList();
        setPartitions(partitions);
        FileReferenceFactory fileReferenceFactory = fileReferenceFactory();
        FileReference fileReference1 = fileReferenceFactory.partitionFile("A", "file1", 200L);
        FileReference fileReference2 = fileReferenceFactory.partitionFile("A", "file2", 200L);
        setFileReferences(List.of(fileReference1, fileReference2));

        // When
        List<CompactionJob> jobs = createJobs();

        // Then
        assertThat(jobs).satisfiesExactlyInAnyOrder(job -> {
            assertThat(job).isEqualTo(CompactionJob.builder()
                    .jobId(job.getId())
                    .tableId(tableProperties.get(TABLE_ID))
                    .inputFiles(List.of("file1", "file2"))
                    .outputFile(job.getOutputFile())
                    .partitionId("B")
                    .build());
            verifySetJobForFilesInStateStore(job.getId(), List.of(
                    referenceForChildPartition(fileReference1, "B"),
                    referenceForChildPartition(fileReference2, "B")));
            verifyJobCreationReported(job);
        }, job -> {
            assertThat(job).isEqualTo(CompactionJob.builder()
                    .jobId(job.getId())
                    .tableId(tableProperties.get(TABLE_ID))
                    .inputFiles(List.of("file1", "file2"))
                    .outputFile(job.getOutputFile())
                    .partitionId("C")
                    .build());
            verifySetJobForFilesInStateStore(job.getId(), List.of(
                    referenceForChildPartition(fileReference1, "C"),
                    referenceForChildPartition(fileReference2, "C")
            ));
            verifyJobCreationReported(job);
        });
    }

    @Test
    public void shouldCreateCompactionJobsToConvertSplitFilesToWholeFiles() throws Exception {
        // Given
        tableProperties.set(COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName());
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "1");
        List<Partition> partitions = new PartitionsBuilder(schema)
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "ddd")
                .buildList();
        setPartitions(partitions);
        FileReferenceFactory fileReferenceFactory = fileReferenceFactory();
        FileReference fileReference = fileReferenceFactory.partitionFile("A", "file", 200L);
        FileReference fileReferenceLeft = referenceForChildPartition(fileReference, "B");
        FileReference fileReferenceRight = referenceForChildPartition(fileReference, "C");
        setFileReferences(List.of(fileReferenceLeft, fileReferenceRight));

        // When
        List<CompactionJob> jobs = createJobs();

        // Then
        assertThat(jobs).satisfiesExactlyInAnyOrder(job -> {
            assertThat(job).isEqualTo(CompactionJob.builder()
                    .jobId(job.getId())
                    .tableId(tableProperties.get(TABLE_ID))
                    .inputFiles(List.of(fileReferenceLeft.getFilename()))
                    .outputFile(job.getOutputFile())
                    .partitionId("B")
                    .build());
            verifySetJobForFilesInStateStore(job.getId(), List.of(fileReferenceLeft));
            verifyJobCreationReported(job);
        }, job -> {
            assertThat(job).isEqualTo(CompactionJob.builder()
                    .jobId(job.getId())
                    .tableId(tableProperties.get(TABLE_ID))
                    .inputFiles(List.of(fileReferenceRight.getFilename()))
                    .outputFile(job.getOutputFile())
                    .partitionId("C")
                    .build());
            verifySetJobForFilesInStateStore(job.getId(), List.of(fileReferenceRight));
            verifyJobCreationReported(job);
        });
    }

    @Test
    void shouldCreateJobsWhenStrategyDoesNotCreateJobsForWholeFilesWithForceCreateJobsFlagSet() throws Exception {
        // Given we use the BasicCompactionStrategy with a batch size of 3
        tableProperties.set(COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName());
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "3");
        setPartitions(new PartitionsBuilder(schema).singlePartition("root").buildList());
        FileReferenceFactory fileReferenceFactory = fileReferenceFactory();
        // And we have 2 active whole files in the state store (which the BasicCompactionStrategy will skip
        // as it does not create jobs with fewer files than the batch size)
        FileReference fileReference1 = fileReferenceFactory.rootFile("file1", 200L);
        FileReference fileReference2 = fileReferenceFactory.rootFile("file2", 200L);
        setFileReferences(List.of(fileReference1, fileReference2));

        // When we force create jobs
        List<CompactionJob> jobs = forceCreateJobs();

        // Then a compaction job will be created for the files skipped by the BasicCompactionStrategy
        assertThat(jobs).satisfiesExactly(job -> {
            assertThat(job).isEqualTo(CompactionJob.builder()
                    .jobId(job.getId())
                    .tableId(tableProperties.get(TABLE_ID))
                    .inputFiles(List.of("file1", "file2"))
                    .outputFile(job.getOutputFile())
                    .partitionId("root")
                    .build());
            verifySetJobForFilesInStateStore(job.getId(), List.of(fileReference1, fileReference2));
            verifyJobCreationReported(job);
        });
    }

    @Test
    void shouldCreateJobsWhenStrategyDoesNotCreateJobsForSplitFilesWithForceCreateJobsFlagSet() throws Exception {
        // Given we use the BasicCompactionStrategy with a batch size of 3
        tableProperties.set(COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName());
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "3");
        setPartitions(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "aaa")
                .buildList());
        FileReferenceFactory fileReferenceFactory = fileReferenceFactory();
        // And we have 1 active file that has been split in the state store (which the BasicCompactionStrategy
        // will skip as it does not create jobs with fewer files than the batch size)
        FileReference rootFile = fileReferenceFactory.rootFile("file1", 2L);
        FileReference fileReference1 = referenceForChildPartition(rootFile, "L");
        setFileReferences(List.of(fileReference1));

        // When we force create jobs
        List<CompactionJob> jobs = forceCreateJobs();

        // Then a compaction job will be created for the files skipped by the BasicCompactionStrategy
        assertThat(jobs).satisfiesExactly(job -> {
            assertThat(job).isEqualTo(CompactionJob.builder()
                    .jobId(job.getId())
                    .tableId(tableProperties.get(TABLE_ID))
                    .inputFiles(List.of("file1"))
                    .outputFile(job.getOutputFile())
                    .partitionId("L")
                    .build());
            verifySetJobForFilesInStateStore(job.getId(), List.of(fileReference1));
            verifyJobCreationReported(job);
        });
    }

    private FileReferenceFactory fileReferenceFactory() {
        return FileReferenceFactory.from(stateStore);
    }

    private void setPartitions(List<Partition> partitions) throws Exception {
        stateStore.initialise(partitions);
    }

    private void setFileReferences(List<FileReference> fileReferences) throws Exception {
        stateStore.addFiles(fileReferences);
    }

    private void verifySetJobForFilesInStateStore(String jobId, List<FileReference> fileReferences) {
        assertThat(fileReferences).allSatisfy(fileReference ->
                assertThat(getActiveStateFromStateStore(fileReference).getJobId()).isEqualTo(jobId));
    }

    private FileReference getActiveStateFromStateStore(FileReference fileReference) throws Exception {
        List<FileReference> foundRecords = stateStore.getFileReferences().stream()
                .filter(found -> found.getPartitionId().equals(fileReference.getPartitionId()))
                .filter(found -> found.getFilename().equals(fileReference.getFilename()))
                .collect(Collectors.toUnmodifiableList());
        if (foundRecords.size() != 1) {
            throw new IllegalStateException("Expected one matching file reference, found: " + foundRecords);
        }
        return foundRecords.get(0);
    }

    private void verifyJobCreationReported(CompactionJob job) {
        assertThat(jobStatusStore.getJob(job.getId()).orElseThrow())
                .usingRecursiveComparison().ignoringFields("createdStatus.updateTime")
                .isEqualTo(jobCreated(job, Instant.MAX));
    }

    private List<CompactionJob> createJobs() throws Exception {
        List<CompactionJob> compactionJobs = new ArrayList<>();
        CreateCompactionJobs createJobs = CreateCompactionJobs.standard(ObjectFactory.noUserJars(), instanceProperties,
                new FixedTablePropertiesProvider(tableProperties),
                new FixedStateStoreProvider(tableProperties, stateStore),
                compactionJobs::add, jobStatusStore);
        createJobs.createJobs();
        return compactionJobs;
    }

    private List<CompactionJob> forceCreateJobs() throws Exception {
        List<CompactionJob> compactionJobs = new ArrayList<>();
        CreateCompactionJobs createJobs = CreateCompactionJobs.compactAllFiles(ObjectFactory.noUserJars(), instanceProperties,
                new FixedTablePropertiesProvider(tableProperties),
                new FixedStateStoreProvider(tableProperties, stateStore),
                compactionJobs::add, jobStatusStore);
        createJobs.createJobs();
        return compactionJobs;
    }
}
