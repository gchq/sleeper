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
package sleeper.compaction.strategy.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class BasicCompactionStrategyTest {

    private static final Schema DEFAULT_SCHEMA = schemaWithKey("key");
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, DEFAULT_SCHEMA);

    private CompactionJob.Builder jobForTable() {
        return CompactionJob.builder().tableId("table-id");
    }

    @BeforeEach
    void setUp() {
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(CONFIG_BUCKET, "bucket");
        instanceProperties.set(DATA_BUCKET, "databucket");
        tableProperties.set(TABLE_NAME, "table");
        tableProperties.set(TABLE_ID, "table-id");
    }

    @Test
    public void shouldCreateOneJobWhenOneLeafPartitionAndOnlyTwoFiles() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "2");
        BasicCompactionStrategy strategy = new BasicCompactionStrategy();
        PartitionTree partitionTree = new PartitionsBuilder(DEFAULT_SCHEMA)
                .singlePartition("root")
                .buildTree();
        FileReferenceFactory factory = FileReferenceFactory.from(partitionTree);
        FileReference fileReference1 = factory.rootFile("file1", 100L);
        FileReference fileReference2 = factory.rootFile("file2", 100L);
        List<FileReference> fileReferences = List.of(fileReference1, fileReference2);
        CompactionJobFactory jobFactory = fixJobIds(List.of("job1"));

        // When
        List<CompactionJob> compactionJobs = strategy.createCompactionJobs(
                instanceProperties, tableProperties, jobFactory, fileReferences, partitionTree.getAllPartitions());

        // Then
        assertThat(compactionJobs).containsExactly(
                jobFactory.createCompactionJob("job1", fileReferences, "root"));
    }

    @Test
    public void shouldCreateCorrectJobsWhenOneLeafPartitionAndLotsOfFiles() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "10");
        BasicCompactionStrategy strategy = new BasicCompactionStrategy();
        PartitionTree partitionTree = new PartitionsBuilder(DEFAULT_SCHEMA)
                .singlePartition("root")
                .buildTree();
        FileReferenceFactory factory = FileReferenceFactory.from(partitionTree);
        List<FileReference> fileReferences = new ArrayList<>();
        List<FileReference> filesInAscendingOrder = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            FileReference fileReference = factory.rootFile("file-" + i, 1_000_000L - i * 100L);
            fileReferences.add(fileReference);
            filesInAscendingOrder.add(0, fileReference);
        }
        CompactionJobFactory jobFactory = fixJobIds(
                IntStream.iterate(1, i -> i + 1)
                        .mapToObj(i -> "job" + i)
                        .iterator()::next);

        // When
        List<CompactionJob> compactionJobs = strategy.createCompactionJobs(
                instanceProperties, tableProperties, jobFactory, fileReferences, partitionTree.getAllPartitions());

        // Then

        assertThat(compactionJobs).containsExactly(
                jobFactory.createCompactionJob("job1", filesInAscendingOrder.subList(0, 10), "root"),
                jobFactory.createCompactionJob("job2", filesInAscendingOrder.subList(10, 20), "root"),
                jobFactory.createCompactionJob("job3", filesInAscendingOrder.subList(20, 30), "root"),
                jobFactory.createCompactionJob("job4", filesInAscendingOrder.subList(30, 40), "root"),
                jobFactory.createCompactionJob("job5", filesInAscendingOrder.subList(40, 50), "root"),
                jobFactory.createCompactionJob("job6", filesInAscendingOrder.subList(50, 60), "root"),
                jobFactory.createCompactionJob("job7", filesInAscendingOrder.subList(60, 70), "root"),
                jobFactory.createCompactionJob("job8", filesInAscendingOrder.subList(70, 80), "root"),
                jobFactory.createCompactionJob("job9", filesInAscendingOrder.subList(80, 90), "root"),
                jobFactory.createCompactionJob("job10", filesInAscendingOrder.subList(90, 100), "root"));
    }

    @Test
    public void shouldCreateNoJobsWhenNotEnoughFiles() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "5");
        BasicCompactionStrategy strategy = new BasicCompactionStrategy();
        PartitionTree partitionTree = new PartitionsBuilder(DEFAULT_SCHEMA)
                .singlePartition("root")
                .buildTree();
        FileReferenceFactory factory = FileReferenceFactory.from(partitionTree);
        FileReference fileReference1 = factory.rootFile("file1", 100L);
        FileReference fileReference2 = factory.rootFile("file2", 100L);
        List<FileReference> fileReferences = List.of(fileReference1, fileReference2);

        // When
        List<CompactionJob> compactionJobs = strategy.createCompactionJobs(
                instanceProperties, tableProperties, fixJobIds(List.of("job1")), fileReferences, partitionTree.getAllPartitions());

        // Then
        assertThat(compactionJobs).isEmpty();
    }

    @Test
    public void shouldCreateNoJobsWhenFileInLeafPartitionIsAssignedToAJob() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "5");
        BasicCompactionStrategy strategy = new BasicCompactionStrategy();
        PartitionTree partitionTree = new PartitionsBuilder(DEFAULT_SCHEMA)
                .singlePartition("root")
                .buildTree();
        FileReference fileReference = FileReference.builder()
                .filename("file1.parquet")
                .partitionId("root")
                .jobId("test-job")
                .numberOfRecords(123L)
                .build();

        // When
        List<CompactionJob> compactionJobs = strategy.createCompactionJobs(
                instanceProperties, tableProperties, fixJobIds(List.of("job1")), List.of(fileReference), partitionTree.getAllPartitions());

        // Then
        assertThat(compactionJobs).isEmpty();
    }

    @Test
    public void shouldCreateJobsWhenMultiplePartitions() {
        // Given - 3 partitions (root and 2 children) - the child partition called "left" has files for 2 compaction
        // jobs, the "right" child partition only has files for 1 compaction job
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "2");
        BasicCompactionStrategy strategy = new BasicCompactionStrategy();
        PartitionTree partitionTree = new PartitionsBuilder(DEFAULT_SCHEMA)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 123L)
                .buildTree();
        FileReferenceFactory factory = FileReferenceFactory.from(partitionTree);
        FileReference fileReference1 = factory.partitionFile("left", "file1", 100L);
        FileReference fileReference2 = factory.partitionFile("left", "file2", 200L);
        FileReference fileReference3 = factory.partitionFile("left", "file3", 300L);
        FileReference fileReference4 = factory.partitionFile("left", "file4", 400L);
        FileReference fileReference5 = factory.partitionFile("right", "file5", 500L);
        FileReference fileReference6 = factory.partitionFile("right", "file6", 600L);
        List<FileReference> fileReferences = List.of(
                fileReference1, fileReference2, fileReference3, fileReference4, fileReference5, fileReference6);
        CompactionJobFactory jobFactory = fixJobIds(List.of("job1", "job2", "job3", "job4"));

        // When
        List<CompactionJob> compactionJobs = strategy.createCompactionJobs(
                instanceProperties, tableProperties, jobFactory, fileReferences, partitionTree.getAllPartitions());

        // Then
        assertThat(compactionJobs).containsExactly(
                jobFactory.createCompactionJob("job1", List.of(fileReference1, fileReference2), "left"),
                jobFactory.createCompactionJob("job2", List.of(fileReference3, fileReference4), "left"),
                jobFactory.createCompactionJob("job3", List.of(fileReference5, fileReference6), "right"));
    }

    private CompactionJobFactory fixJobIds(List<String> jobIds) {
        return fixJobIds(jobIds.iterator()::next);
    }

    private CompactionJobFactory fixJobIds(Supplier<String> jobIdSupplier) {
        return new CompactionJobFactory(instanceProperties, tableProperties, jobIdSupplier);
    }
}
