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
package sleeper.compaction.job.creation;

import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.testutils.CompactionJobStatusStoreInMemory;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.FixedStateStoreProvider;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.compaction.job.creation.CreateJobsTestUtils.createInstanceProperties;
import static sleeper.compaction.job.creation.CreateJobsTestUtils.createTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithNoPartitions;

public class CreateJobsTest {


    private final InstanceProperties instanceProperties = createInstanceProperties();
    private final Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
    private final TableProperties tableProperties = createTableProperties(schema, instanceProperties);
    private final StateStore stateStore = inMemoryStateStoreWithNoPartitions();
    private final CompactionJobStatusStore jobStatusStore = new CompactionJobStatusStoreInMemory();

    @Test
    public void shouldCompactAllFilesInSinglePartition() throws Exception {
        // Given
        setPartitions(new PartitionsBuilder(schema).singlePartition("root").buildList());
        FileInfoFactory fileInfoFactory = fileInfoFactory();
        FileInfo fileInfo1 = fileInfoFactory.rootFile("file1", 200L);
        FileInfo fileInfo2 = fileInfoFactory.rootFile("file2", 200L);
        FileInfo fileInfo3 = fileInfoFactory.rootFile("file3", 200L);
        FileInfo fileInfo4 = fileInfoFactory.rootFile("file4", 200L);
        List<FileInfo> files = List.of(fileInfo1, fileInfo2, fileInfo3, fileInfo4);
        setActiveFiles(files);

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
                    .isSplittingJob(false)
                    .build());
            verifySetJobForFilesInStateStore(job.getId(), files);
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
        FileInfoFactory fileInfoFactory = fileInfoFactory();
        FileInfo fileInfo1 = fileInfoFactory.partitionFile("B", "file1", 200L);
        FileInfo fileInfo2 = fileInfoFactory.partitionFile("B", "file2", 200L);
        FileInfo fileInfo3 = fileInfoFactory.partitionFile("C", "file3", 200L);
        FileInfo fileInfo4 = fileInfoFactory.partitionFile("C", "file4", 200L);
        setActiveFiles(List.of(fileInfo1, fileInfo2, fileInfo3, fileInfo4));

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
                    .isSplittingJob(false)
                    .build());
            verifySetJobForFilesInStateStore(job.getId(), List.of(fileInfo1, fileInfo2));
            verifyJobCreationReported(job);
        }, job -> {
            assertThat(job).isEqualTo(CompactionJob.builder()
                    .jobId(job.getId())
                    .tableId(tableProperties.get(TABLE_ID))
                    .inputFiles(List.of("file3", "file4"))
                    .outputFile(job.getOutputFile())
                    .partitionId("C")
                    .isSplittingJob(false)
                    .build());
            verifySetJobForFilesInStateStore(job.getId(), List.of(fileInfo3, fileInfo4));
            verifyJobCreationReported(job);
        });
    }

    @Test
    public void shouldCreateSplittingCompaction() throws Exception {
        // Given
        List<Partition> partitions = new PartitionsBuilder(schema)
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "ddd")
                .buildList();
        setPartitions(partitions);
        FileInfoFactory fileInfoFactory = fileInfoFactory();
        FileInfo fileInfo1 = fileInfoFactory.partitionFile("A", "file1", 200L);
        FileInfo fileInfo2 = fileInfoFactory.partitionFile("A", "file2", 200L);
        setActiveFiles(List.of(fileInfo1, fileInfo2));

        // When
        List<CompactionJob> jobs = createJobs();

        // Then
        assertThat(jobs).singleElement().satisfies(job -> {
            assertThat(job).isEqualTo(CompactionJob.builder()
                    .jobId(job.getId())
                    .tableId(tableProperties.get(TABLE_ID))
                    .inputFiles(List.of("file1", "file2"))
                    .outputFiles(job.getOutputFiles())
                    .partitionId("A")
                    .isSplittingJob(true)
                    .childPartitions(List.of("B", "C"))
                    .splitPoint("ddd").dimension(0)
                    .build());
            verifySetJobForFilesInStateStore(job.getId(), List.of(fileInfo1, fileInfo2));
            verifyJobCreationReported(job);
        });
    }

    private FileInfoFactory fileInfoFactory() {
        return FileInfoFactory.from(schema, stateStore);
    }

    private void setPartitions(List<Partition> partitions) throws Exception {
        stateStore.initialise(partitions);
    }

    private void setActiveFiles(List<FileInfo> files) throws Exception {
        stateStore.addFiles(files);
    }

    private void verifySetJobForFilesInStateStore(String jobId, List<FileInfo> files) {
        assertThat(files).allSatisfy(file ->
                assertThat(getActiveStateFromStateStore(file).getJobId()).isEqualTo(jobId));
    }

    private FileInfo getActiveStateFromStateStore(FileInfo file) throws Exception {
        List<FileInfo> foundRecords = stateStore.getActiveFiles().stream()
                .filter(found -> found.getFilename().equals(file.getFilename()))
                .collect(Collectors.toUnmodifiableList());
        if (foundRecords.size() != 1) {
            throw new IllegalStateException("Expected one matching active file, found: " + foundRecords);
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
        CreateJobs createJobs = new CreateJobs(ObjectFactory.noUserJars(), instanceProperties,
                new FixedTablePropertiesProvider(tableProperties),
                new FixedStateStoreProvider(tableProperties, stateStore),
                compactionJobs::add, jobStatusStore);
        createJobs.createJobs();
        return compactionJobs;
    }
}
