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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static sleeper.compaction.job.creation.CreateJobsTestUtils.createInstanceProperties;
import static sleeper.compaction.job.creation.CreateJobsTestUtils.createTableProperties;

public class CreateJobsTest {

    private final StateStore stateStore = mock(StateStore.class);
    private final CompactionJobStatusStore jobStatusStore = mock(CompactionJobStatusStore.class);
    private final Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();

    @Test
    public void shouldCompactAllFilesInSinglePartition() throws Exception {
        // Given
        Partition partition = setSinglePartition();
        FileInfoFactory fileInfoFactory = new FileInfoFactory(schema, Collections.singletonList(partition), Instant.now());
        FileInfo fileInfo1 = fileInfoFactory.leafFile("file1", 200L, "a", "b");
        FileInfo fileInfo2 = fileInfoFactory.leafFile("file2", 200L, "c", "d");
        FileInfo fileInfo3 = fileInfoFactory.leafFile("file3", 200L, "e", "f");
        FileInfo fileInfo4 = fileInfoFactory.leafFile("file4", 200L, "g", "h");
        List<FileInfo> files = Arrays.asList(fileInfo1, fileInfo2, fileInfo3, fileInfo4);
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
        verifyOtherStateStoreCalls();
        verifyNoMoreJobCreationReports();
    }

    @Test
    public void shouldCompactFilesInDifferentPartitions() throws Exception {
        // Given
        List<Partition> partitions = new PartitionsBuilder(schema)
                .leavesWithSplits(
                        Arrays.asList("A", "B"),
                        Collections.singletonList("ddd"))
                .parentJoining("C", "A", "B")
                .buildList();
        setPartitions(partitions);
        FileInfoFactory fileInfoFactory = new FileInfoFactory(schema, partitions, Instant.now());
        FileInfo fileInfo1 = fileInfoFactory.leafFile("file1", 200L, "a", "b");
        FileInfo fileInfo2 = fileInfoFactory.leafFile("file2", 200L, "c", "d");
        FileInfo fileInfo3 = fileInfoFactory.leafFile("file3", 200L, "e", "f");
        FileInfo fileInfo4 = fileInfoFactory.leafFile("file4", 200L, "g", "h");
        setActiveFiles(Arrays.asList(fileInfo1, fileInfo2, fileInfo3, fileInfo4));

        // When
        List<CompactionJob> jobs = createJobs();

        // Then
        assertThat(jobs).satisfiesExactlyInAnyOrder(job -> {
            verifySetJobForFilesInStateStore(job.getId(), Arrays.asList(fileInfo1, fileInfo2));
            assertThat(job.getInputFiles()).containsExactlyInAnyOrder("file1", "file2");
            assertThat(job.getPartitionId()).isEqualTo("A");
            assertThat(job.isSplittingJob()).isFalse();
            verifyJobCreationReported(job);
        }, job -> {
            verifySetJobForFilesInStateStore(job.getId(), Arrays.asList(fileInfo3, fileInfo4));
            assertThat(job.getInputFiles()).containsExactlyInAnyOrder("file3", "file4");
            assertThat(job.getPartitionId()).isEqualTo("B");
            assertThat(job.isSplittingJob()).isFalse();
            verifyJobCreationReported(job);
        });
        verifyOtherStateStoreCalls();
        verifyNoMoreJobCreationReports();
    }

    private Partition setSinglePartition() throws Exception {
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct();
        setPartitions(partitions);
        return partitions.get(0);
    }

    private void setPartitions(List<Partition> partitions) throws Exception {
        when(stateStore.getAllPartitions()).thenReturn(partitions);
    }

    private void setActiveFiles(List<FileInfo> files) throws Exception {
        when(stateStore.getActiveFiles()).thenReturn(files);
    }

    private void verifySetJobForFilesInStateStore(String jobId, List<FileInfo> files) throws Exception {
        verify(stateStore).atomicallyUpdateJobStatusOfFiles(
                eq(jobId), argThat(actualFiles -> {
                    assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(files);
                    return true;
                }));
    }

    private void verifyOtherStateStoreCalls() throws Exception {
        verify(stateStore).getAllPartitions();
        verify(stateStore).getActiveFiles();
        verifyNoMoreInteractions(stateStore);
    }

    private void verifyJobCreationReported(CompactionJob job) {
        verify(jobStatusStore).jobCreated(job);
    }

    private void verifyNoMoreJobCreationReports() {
        verifyNoMoreInteractions(jobStatusStore);
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
