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
package sleeper.compaction.strategy.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.statestore.FileInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class BasicCompactionStrategyTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);

    private CompactionJob.Builder jobForTable() {
        return CompactionJob.builder().tableId("table-id");
    }

    @BeforeEach
    void setUp() {
        instanceProperties.set(CONFIG_BUCKET, "bucket");
        instanceProperties.set(DATA_BUCKET, "databucket");
        tableProperties.set(TABLE_NAME, "table");
        tableProperties.set(TABLE_ID, "table-id");
    }

    @Test
    public void shouldCreateOneJobWhenOneLeafPartitionAndOnlyTwoFiles() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "2");
        BasicCompactionStrategy basicCompactionStrategy = new BasicCompactionStrategy();
        basicCompactionStrategy.init(instanceProperties, tableProperties);
        Partition partition = Partition.builder()
                .id("root")
                .rowKeyTypes(new IntType())
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(Collections.emptyList())
                .build();
        List<Partition> partitions = Collections.singletonList(partition);
        List<FileInfo> fileInfos = new ArrayList<>();
        FileInfo fileInfo1 = FileInfo.wholeFile()
                .filename("file1")
                .partitionId(partition.getId())
                .numberOfRecords(100L)
                .build();
        fileInfos.add(fileInfo1);
        FileInfo fileInfo2 = FileInfo.wholeFile()
                .filename("file2")
                .partitionId(partition.getId())
                .numberOfRecords(100L)
                .build();
        fileInfos.add(fileInfo2);

        // When
        List<CompactionJob> compactionJobs = basicCompactionStrategy.createCompactionJobs(Collections.emptyList(), fileInfos, partitions);

        // Then
        assertThat(compactionJobs).hasSize(1);
        CompactionJob expectedCompactionJob = jobForTable()
                .jobId(compactionJobs.get(0).getId()) // Job id is a UUID so we don't know what it will be
                .partitionId(partition.getId())
                .inputFiles(Arrays.asList(fileInfo1.getFilename(), fileInfo2.getFilename()))
                .isSplittingJob(false)
                .outputFile(instanceProperties.get(FILE_SYSTEM) + "databucket/table-id/partition_" + partition.getId() + "/" + compactionJobs.get(0).getId() + ".parquet")
                .iteratorClassName(null)
                .iteratorConfig(null).build();
        assertThat(compactionJobs).containsExactly(expectedCompactionJob);
    }

    @Test
    public void shouldCreateCorrectJobsWhenOneLeafPartitionAndLotsOfFiles() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "10");
        BasicCompactionStrategy basicCompactionStrategy = new BasicCompactionStrategy();
        basicCompactionStrategy.init(instanceProperties, tableProperties);
        Partition partition = Partition.builder()
                .id("root")
                .rowKeyTypes(new IntType())
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(Collections.emptyList())
                .build();
        List<Partition> partitions = Collections.singletonList(partition);
        List<FileInfo> fileInfos = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            FileInfo fileInfo = FileInfo.wholeFile()
                    .filename("file-" + i)
                    .partitionId(partition.getId())
                    .numberOfRecords(1_000_000L - i * 100L)
                    .build();
            fileInfos.add(fileInfo);
        }

        // When
        List<CompactionJob> compactionJobs = basicCompactionStrategy.createCompactionJobs(Collections.emptyList(), fileInfos, partitions);

        // Then
        assertThat(compactionJobs).hasSize(10).isEqualTo(IntStream.range(0, 10).mapToObj(i -> {
            List<String> inputFiles = new ArrayList<>();
            for (int j = 99 - i * 10; j > 99 - (i + 1) * 10; j--) {
                inputFiles.add("file-" + j);
            }
            return jobForTable()
                    .jobId(compactionJobs.get(i).getId()) // Job id is a UUID so we don't know what it will be
                    .partitionId(partition.getId())
                    .inputFiles(inputFiles)
                    .isSplittingJob(false)
                    .outputFile(instanceProperties.get(FILE_SYSTEM) + "databucket/table-id/partition_" + partition.getId() + "/" + compactionJobs.get(i).getId() + ".parquet")
                    .iteratorClassName(null)
                    .iteratorConfig(null).build();
        }).collect(Collectors.toList()));
    }

    @Test
    public void shouldCreateNoJobsWhenNotEnoughFiles() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "5");
        BasicCompactionStrategy basicCompactionStrategy = new BasicCompactionStrategy();
        basicCompactionStrategy.init(instanceProperties, tableProperties);
        Partition partition = Partition.builder()
                .id("root")
                .rowKeyTypes(new IntType())
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(Collections.emptyList())
                .build();
        List<Partition> partitions = Collections.singletonList(partition);
        List<FileInfo> fileInfos = new ArrayList<>();
        FileInfo fileInfo1 = FileInfo.wholeFile()
                .filename("file1")
                .partitionId(partition.getId())
                .numberOfRecords(100L)
                .build();
        fileInfos.add(fileInfo1);
        FileInfo fileInfo2 = FileInfo.wholeFile()
                .filename("file2")
                .partitionId(partition.getId())
                .numberOfRecords(100L)
                .build();
        fileInfos.add(fileInfo2);

        // When
        List<CompactionJob> compactionJobs = basicCompactionStrategy.createCompactionJobs(Collections.emptyList(), fileInfos, partitions);

        // Then
        assertThat(compactionJobs).isEmpty();
    }

    @Test
    public void shouldCreateJobsWhenMultiplePartitions() {
        // Given - 3 partitions (root and 2 children) - the child partition called "left" has files for 2 compaction
        // jobs, the "right" child partition only has files for 1 compaction job
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "2");
        BasicCompactionStrategy basicCompactionStrategy = new BasicCompactionStrategy();
        basicCompactionStrategy.init(instanceProperties, tableProperties);
        Partition rootPartition = Partition.builder()
                .id("root")
                .rowKeyTypes(new IntType())
                .leafPartition(false)
                .parentPartitionId(null)
                .childPartitionIds(Arrays.asList("left", "right"))
                .build();
        Partition leftChild = Partition.builder()
                .id("left")
                .rowKeyTypes(new IntType())
                .leafPartition(true)
                .parentPartitionId("root")
                .childPartitionIds(Collections.emptyList())
                .build();
        Partition rightChild = Partition.builder()
                .id("right")
                .rowKeyTypes(new IntType())
                .leafPartition(true)
                .parentPartitionId("root")
                .childPartitionIds(Collections.emptyList())
                .build();
        List<Partition> partitions = Arrays.asList(rootPartition, leftChild, rightChild);
        List<FileInfo> fileInfos = new ArrayList<>();
        FileInfo fileInfo1 = FileInfo.wholeFile()
                .filename("file1")
                .partitionId("left")
                .numberOfRecords(100L)
                .build();
        fileInfos.add(fileInfo1);
        FileInfo fileInfo2 = FileInfo.wholeFile()
                .filename("file2")
                .partitionId("left")
                .numberOfRecords(200L)
                .build();
        fileInfos.add(fileInfo2);
        FileInfo fileInfo3 = FileInfo.wholeFile()
                .filename("file3")
                .partitionId("left")
                .numberOfRecords(300L)
                .build();
        fileInfos.add(fileInfo3);
        FileInfo fileInfo4 = FileInfo.wholeFile()
                .filename("file4")
                .partitionId("left")
                .numberOfRecords(400L)
                .build();
        fileInfos.add(fileInfo4);
        FileInfo fileInfo5 = FileInfo.wholeFile()
                .filename("file5")
                .partitionId("right")
                .numberOfRecords(500L)
                .build();
        fileInfos.add(fileInfo5);
        FileInfo fileInfo6 = FileInfo.wholeFile()
                .filename("file6")
                .partitionId("right")
                .numberOfRecords(600L)
                .build();
        fileInfos.add(fileInfo6);

        // When
        List<CompactionJob> compactionJobs = basicCompactionStrategy.createCompactionJobs(Collections.emptyList(), fileInfos, partitions);

        // Then
        assertThat(compactionJobs).hasSize(3);
        CompactionJob expectedCompactionJob1 = jobForTable()
                .jobId(compactionJobs.get(0).getId()) // Job id is a UUID so we don't know what it will be
                .partitionId("left")
                .inputFiles(Arrays.asList(fileInfo1.getFilename(), fileInfo2.getFilename()))
                .isSplittingJob(false)
                .outputFile(instanceProperties.get(FILE_SYSTEM) + "databucket/table-id/partition_left/" + compactionJobs.get(0).getId() + ".parquet")
                .iteratorClassName(null)
                .iteratorConfig(null).build();
        CompactionJob expectedCompactionJob2 = jobForTable()
                .jobId(compactionJobs.get(1).getId()) // Job id is a UUID so we don't know what it will be
                .partitionId("left")
                .inputFiles(Arrays.asList(fileInfo3.getFilename(), fileInfo4.getFilename()))
                .isSplittingJob(false)
                .outputFile(instanceProperties.get(FILE_SYSTEM) + "databucket/table-id/partition_left/" + compactionJobs.get(1).getId() + ".parquet")
                .iteratorClassName(null)
                .iteratorConfig(null).build();
        CompactionJob expectedCompactionJob3 = jobForTable()
                .jobId(compactionJobs.get(2).getId()) // Job id is a UUID so we don't know what it will be
                .partitionId("right")
                .inputFiles(Arrays.asList(fileInfo5.getFilename(), fileInfo6.getFilename()))
                .isSplittingJob(false)
                .outputFile(instanceProperties.get(FILE_SYSTEM) + "databucket/table-id/partition_right/" + compactionJobs.get(2).getId() + ".parquet")
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
        BasicCompactionStrategy basicCompactionStrategy = new BasicCompactionStrategy();
        basicCompactionStrategy.init(instanceProperties, tableProperties);
        Range rootRange = new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, null);
        Partition rootPartition = Partition.builder()
                .id("root")
                .rowKeyTypes(new IntType())
                .leafPartition(false)
                .parentPartitionId(null)
                .childPartitionIds(Arrays.asList("left", "right"))
                .region(new Region(rootRange))
                .dimension(0)
                .build();
        Range leftRange = new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, 10);
        Partition leftChild = Partition.builder()
                .id("left")
                .rowKeyTypes(new IntType())
                .leafPartition(true)
                .parentPartitionId("root")
                .childPartitionIds(Collections.emptyList())
                .region(new Region(leftRange))
                .build();
        Range rightRange = new RangeFactory(schema).createRange(field, 10, null);
        Partition rightChild = Partition.builder()
                .id("right")
                .rowKeyTypes(new IntType())
                .leafPartition(true)
                .parentPartitionId("root")
                .childPartitionIds(Collections.emptyList())
                .region(new Region(rightRange))
                .build();
        List<Partition> partitions = Arrays.asList(rootPartition, leftChild, rightChild);
        List<FileInfo> fileInfos = new ArrayList<>();
        FileInfo fileInfo1 = FileInfo.wholeFile()
                .filename("file1")
                .partitionId("root")
                .numberOfRecords(100L)
                .build();
        fileInfos.add(fileInfo1);
        FileInfo fileInfo2 = FileInfo.wholeFile()
                .filename("file2")
                .partitionId("root")
                .numberOfRecords(200L)
                .build();
        fileInfos.add(fileInfo2);

        // When
        List<CompactionJob> compactionJobs = basicCompactionStrategy.createCompactionJobs(Collections.emptyList(), fileInfos, partitions);

        // Then
        assertThat(compactionJobs).hasSize(1);
        CompactionJob expectedCompactionJob = jobForTable()
                .jobId(compactionJobs.get(0).getId()) // Job id is a UUID so we don't know what it will be
                .partitionId("root")
                .inputFiles(Arrays.asList(fileInfo1.getFilename(), fileInfo2.getFilename()))
                .isSplittingJob(true)
                .childPartitions(Arrays.asList("left", "right"))
                .iteratorClassName(null)
                .iteratorConfig(null).build();
        assertThat(compactionJobs).containsExactly(expectedCompactionJob);
    }
}
