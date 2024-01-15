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
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
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
        strategy.init(instanceProperties, tableProperties);
        PartitionsBuilder partitions = new PartitionsBuilder(DEFAULT_SCHEMA)
                .singlePartition("root");
        FileInfoFactory factory = FileInfoFactory.from(partitions.buildTree());
        FileInfo fileInfo1 = factory.rootFile("file1", 100L);
        FileInfo fileInfo2 = factory.rootFile("file2", 100L);
        List<FileInfo> fileInfos = List.of(fileInfo1, fileInfo2);

        // When
        List<CompactionJob> compactionJobs = strategy.createCompactionJobs(List.of(), fileInfos, partitions.buildList());

        // Then
        assertThat(compactionJobs).hasSize(1);
        CompactionJob expectedCompactionJob = jobForTable()
                .jobId(compactionJobs.get(0).getId()) // Job id is a UUID so we don't know what it will be
                .partitionId("root")
                .inputFiles(List.of("file1", "file2"))
                .isSplittingJob(false)
                .outputFile("file://databucket/table-id/partition_root/" + compactionJobs.get(0).getId() + ".parquet")
                .iteratorClassName(null)
                .iteratorConfig(null).build();
        assertThat(compactionJobs).containsExactly(expectedCompactionJob);
    }

    @Test
    public void shouldCreateCorrectJobsWhenOneLeafPartitionAndLotsOfFiles() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "10");
        BasicCompactionStrategy strategy = new BasicCompactionStrategy();
        strategy.init(instanceProperties, tableProperties);
        PartitionsBuilder partitions = new PartitionsBuilder(DEFAULT_SCHEMA)
                .singlePartition("root");
        FileInfoFactory factory = FileInfoFactory.from(partitions.buildTree());
        List<FileInfo> fileInfos = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            FileInfo fileInfo = factory.rootFile("file-" + i, 1_000_000L - i * 100L);
            fileInfos.add(fileInfo);
        }

        // When
        List<CompactionJob> compactionJobs = strategy.createCompactionJobs(List.of(), fileInfos, partitions.buildList());

        // Then
        assertThat(compactionJobs).hasSize(10).isEqualTo(IntStream.range(0, 10).mapToObj(i -> {
            List<String> inputFiles = new ArrayList<>();
            for (int j = 99 - i * 10; j > 99 - (i + 1) * 10; j--) {
                inputFiles.add("file-" + j);
            }
            return jobForTable()
                    .jobId(compactionJobs.get(i).getId()) // Job id is a UUID so we don't know what it will be
                    .partitionId("root")
                    .inputFiles(inputFiles)
                    .isSplittingJob(false)
                    .outputFile("file://databucket/table-id/partition_root/" + compactionJobs.get(i).getId() + ".parquet")
                    .iteratorClassName(null)
                    .iteratorConfig(null).build();
        }).collect(Collectors.toList()));
    }

    @Test
    public void shouldCreateNoJobsWhenNotEnoughFiles() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "5");
        BasicCompactionStrategy strategy = new BasicCompactionStrategy();
        strategy.init(instanceProperties, tableProperties);
        PartitionsBuilder partitions = new PartitionsBuilder(DEFAULT_SCHEMA)
                .singlePartition("root");
        FileInfoFactory factory = FileInfoFactory.from(partitions.buildTree());
        FileInfo fileInfo1 = factory.rootFile("file1", 100L);
        FileInfo fileInfo2 = factory.rootFile("file2", 100L);
        List<FileInfo> fileInfos = List.of(fileInfo1, fileInfo2);

        // When
        List<CompactionJob> compactionJobs = strategy.createCompactionJobs(List.of(), fileInfos, partitions.buildList());

        // Then
        assertThat(compactionJobs).isEmpty();
    }

    @Test
    public void shouldCreateJobsWhenMultiplePartitions() {
        // Given - 3 partitions (root and 2 children) - the child partition called "left" has files for 2 compaction
        // jobs, the "right" child partition only has files for 1 compaction job
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "2");
        BasicCompactionStrategy strategy = new BasicCompactionStrategy();
        strategy.init(instanceProperties, tableProperties);
        PartitionsBuilder partitions = new PartitionsBuilder(DEFAULT_SCHEMA)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 123L);
        FileInfoFactory factory = FileInfoFactory.from(partitions.buildTree());
        FileInfo fileInfo1 = factory.partitionFile("left", "file1", 100L);
        FileInfo fileInfo2 = factory.partitionFile("left", "file2", 200L);
        FileInfo fileInfo3 = factory.partitionFile("left", "file3", 300L);
        FileInfo fileInfo4 = factory.partitionFile("left", "file4", 400L);
        FileInfo fileInfo5 = factory.partitionFile("right", "file5", 500L);
        FileInfo fileInfo6 = factory.partitionFile("right", "file6", 600L);
        List<FileInfo> fileInfos = List.of(fileInfo1, fileInfo2, fileInfo3, fileInfo4, fileInfo5, fileInfo6);

        // When
        List<CompactionJob> compactionJobs = strategy.createCompactionJobs(List.of(), fileInfos, partitions.buildList());

        // Then
        assertThat(compactionJobs).hasSize(3);
        CompactionJob expectedCompactionJob1 = jobForTable()
                .jobId(compactionJobs.get(0).getId()) // Job id is a UUID so we don't know what it will be
                .partitionId("left")
                .inputFiles(List.of("file1", "file2"))
                .isSplittingJob(false)
                .outputFile("file://databucket/table-id/partition_left/" + compactionJobs.get(0).getId() + ".parquet")
                .iteratorClassName(null)
                .iteratorConfig(null).build();
        CompactionJob expectedCompactionJob2 = jobForTable()
                .jobId(compactionJobs.get(1).getId()) // Job id is a UUID so we don't know what it will be
                .partitionId("left")
                .inputFiles(List.of("file3", "file4"))
                .isSplittingJob(false)
                .outputFile("file://databucket/table-id/partition_left/" + compactionJobs.get(1).getId() + ".parquet")
                .iteratorClassName(null)
                .iteratorConfig(null).build();
        CompactionJob expectedCompactionJob3 = jobForTable()
                .jobId(compactionJobs.get(2).getId()) // Job id is a UUID so we don't know what it will be
                .partitionId("right")
                .inputFiles(List.of("file5", "file6"))
                .isSplittingJob(false)
                .outputFile("file://databucket/table-id/partition_right/" + compactionJobs.get(2).getId() + ".parquet")
                .iteratorClassName(null)
                .iteratorConfig(null).build();
        assertThat(compactionJobs).containsExactly(
                expectedCompactionJob1, expectedCompactionJob2, expectedCompactionJob3);
    }

    @Test
    public void shouldCreateSplittingJobs() {
        // Given
        Field field = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "2");
        tableProperties.setSchema(schema);
        BasicCompactionStrategy strategy = new BasicCompactionStrategy();
        strategy.init(instanceProperties, tableProperties);
        PartitionsBuilder partitions = new PartitionsBuilder(schema)
                .singlePartition("root")
                .splitToNewChildren("root", "left", "right", 10);
        FileInfoFactory factory = FileInfoFactory.from(partitions.buildTree());
        FileInfo fileInfo1 = factory.rootFile("file1", 100L);
        FileInfo fileInfo2 = factory.rootFile("file2", 200L);
        List<FileInfo> fileInfos = List.of(fileInfo1, fileInfo2);

        // When
        List<CompactionJob> compactionJobs = strategy.createCompactionJobs(List.of(), fileInfos, partitions.buildList());

        // Then
        assertThat(compactionJobs).hasSize(1);
        CompactionJob expectedCompactionJob = jobForTable()
                .jobId(compactionJobs.get(0).getId()) // Job id is a UUID so we don't know what it will be
                .partitionId("root")
                .inputFiles(List.of("file1", "file2"))
                .isSplittingJob(true)
                .childPartitions(List.of("left", "right"))
                .iteratorClassName(null)
                .iteratorConfig(null).build();
        assertThat(compactionJobs).containsExactly(expectedCompactionJob);
    }
}
