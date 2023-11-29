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
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.FixedStateStoreProvider;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.compaction.job.creation.CreateJobsTestUtils.createInstanceProperties;
import static sleeper.compaction.job.creation.CreateJobsTestUtils.createTableProperties;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithNoPartitions;

public class CreateJobsTest {

    private final CompactionJobStatusStore jobStatusStore = new CompactionJobStatusStoreInMemory();
    private final Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
    private final StateStore stateStore = inMemoryStateStoreWithNoPartitions();

    @Test
    public void shouldCompactAllFilesInSinglePartition() throws Exception {
        // Given
        Partition partition = setSinglePartition();
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
            verifySetJobForFilesInStateStore(job.getId(), files);
            assertThat(job.getInputFiles()).containsExactlyInAnyOrder("file1", "file2", "file3", "file4");
            assertThat(job.getPartitionId()).isEqualTo(partition.getId());
            assertThat(job.isSplittingJob()).isFalse();
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
            verifySetJobForFilesInStateStore(job.getId(), List.of(fileInfo1, fileInfo2));
            assertThat(job.getInputFiles()).containsExactlyInAnyOrder("file1", "file2");
            assertThat(job.getPartitionId()).isEqualTo("B");
            assertThat(job.isSplittingJob()).isFalse();
            verifyJobCreationReported(job);
        }, job -> {
            verifySetJobForFilesInStateStore(job.getId(), List.of(fileInfo3, fileInfo4));
            assertThat(job.getInputFiles()).containsExactlyInAnyOrder("file3", "file4");
            assertThat(job.getPartitionId()).isEqualTo("C");
            assertThat(job.isSplittingJob()).isFalse();
            verifyJobCreationReported(job);
        });
    }

    private Partition setSinglePartition() throws Exception {
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct();
        setPartitions(partitions);
        return partitions.get(0);
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

        InstanceProperties instanceProperties = createInstanceProperties();
        TableProperties tableProperties = createTableProperties(schema, instanceProperties);

        List<CompactionJob> compactionJobs = new ArrayList<>();
        CreateJobs createJobs = new CreateJobs(ObjectFactory.noUserJars(), instanceProperties,
                new FixedTablePropertiesProvider(tableProperties),
                new FixedStateStoreProvider(tableProperties, stateStore),
                compactionJobs::add, jobStatusStore);
        createJobs.createJobs();
        return compactionJobs;
    }
}
