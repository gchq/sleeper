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

import org.apache.commons.lang3.tuple.MutablePair;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.key.Key;
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
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class BasicCompactionStrategyTest {

    @Test
    public void shouldCreateOneJobWhenOneLeafPartitionAndOnlyTwoFiles() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CONFIG_BUCKET, "config");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "table");
        tableProperties.set(DATA_BUCKET, "databucket");
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
        FileInfo fileInfo1 = FileInfo.builder()
                .filename("file1")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(partition.getId())
                .numberOfRecords(100L)
                .rowKeyTypes(new IntType())
                .minRowKey(Key.create(1))
                .maxRowKey(Key.create(100))
                .build();
        fileInfos.add(fileInfo1);
        FileInfo fileInfo2 = FileInfo.builder()
                .filename("file2")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(partition.getId())
                .numberOfRecords(100L)
                .rowKeyTypes(new IntType())
                .minRowKey(Key.create(2))
                .maxRowKey(Key.create(200))
                .build();
        fileInfos.add(fileInfo2);

        // When
        List<CompactionJob> compactionJobs = basicCompactionStrategy.createCompactionJobs(Collections.emptyList(), fileInfos, partitions);

        // Then
        assertThat(compactionJobs).hasSize(1);
        CompactionJob expectedCompactionJob = CompactionJob.builder()
                .tableName("table")
                .jobId(compactionJobs.get(0).getId()) // Job id is a UUID so we don't know what it will be
                .partitionId(partition.getId())
                .inputFiles(Arrays.asList(fileInfo1.getFilename(), fileInfo2.getFilename()))
                .isSplittingJob(false)
                .outputFile(instanceProperties.get(FILE_SYSTEM) + tableProperties.get(DATA_BUCKET) + "/partition_" + partition.getId() + "/" + compactionJobs.get(0).getId() + ".parquet")
                .childPartitions(null)
                .splitPoint(null)
                .dimension(-1)
                .iteratorClassName(null)
                .iteratorConfig(null).build();
        assertThat(compactionJobs).containsExactly(expectedCompactionJob);
    }

    @Test
    public void shouldCreateCorrectJobsWhenOneLeafPartitionAndLotsOfFiles() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CONFIG_BUCKET, "config");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "table");
        tableProperties.set(DATA_BUCKET, "databucket");
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
            FileInfo fileInfo = FileInfo.builder()
                    .filename("file-" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId(partition.getId())
                    .numberOfRecords(1_000_000L - i * 100L)
                    .rowKeyTypes(new IntType())
                    .minRowKey(Key.create(1))
                    .maxRowKey(Key.create(100))
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
            return CompactionJob.builder()
                    .tableName("table")
                    .jobId(compactionJobs.get(i).getId()) // Job id is a UUID so we don't know what it will be
                    .partitionId(partition.getId())
                    .inputFiles(inputFiles)
                    .isSplittingJob(false)
                    .outputFile(instanceProperties.get(FILE_SYSTEM) + tableProperties.get(DATA_BUCKET) + "/partition_" + partition.getId() + "/" + compactionJobs.get(i).getId() + ".parquet")
                    .childPartitions(null)
                    .splitPoint(null)
                    .dimension(-1)
                    .iteratorClassName(null)
                    .iteratorConfig(null).build();
        }).collect(Collectors.toList()));
    }

    @Test
    public void shouldCreateNoJobsWhenNotEnoughFiles() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CONFIG_BUCKET, "bucket");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "table");
        tableProperties.set(DATA_BUCKET, "databucket");
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
        FileInfo fileInfo1 = FileInfo.builder()
                .filename("file1")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(partition.getId())
                .numberOfRecords(100L)
                .rowKeyTypes(new IntType())
                .minRowKey(Key.create(1))
                .maxRowKey(Key.create(100))
                .build();
        fileInfos.add(fileInfo1);
        FileInfo fileInfo2 = FileInfo.builder()
                .filename("file2")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(partition.getId())
                .numberOfRecords(100L)
                .rowKeyTypes(new IntType())
                .minRowKey(Key.create(2))
                .maxRowKey(Key.create(200))
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
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CONFIG_BUCKET, "bucket");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "table");
        tableProperties.set(DATA_BUCKET, "databucket");
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
        FileInfo fileInfo1 = FileInfo.builder()
                .filename("file1")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("left")
                .numberOfRecords(100L)
                .rowKeyTypes(new IntType())
                .minRowKey(Key.create(1))
                .maxRowKey(Key.create(100))
                .build();
        fileInfos.add(fileInfo1);
        FileInfo fileInfo2 = FileInfo.builder()
                .filename("file2")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("left")
                .numberOfRecords(200L)
                .rowKeyTypes(new IntType())
                .minRowKey(Key.create(2))
                .maxRowKey(Key.create(200))
                .build();
        fileInfos.add(fileInfo2);
        FileInfo fileInfo3 = FileInfo.builder()
                .filename("file3")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("left")
                .numberOfRecords(300L)
                .rowKeyTypes(new IntType())
                .minRowKey(Key.create(1))
                .maxRowKey(Key.create(100))
                .build();
        fileInfos.add(fileInfo3);
        FileInfo fileInfo4 = FileInfo.builder()
                .filename("file4")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("left")
                .numberOfRecords(400L)
                .rowKeyTypes(new IntType())
                .minRowKey(Key.create(2))
                .maxRowKey(Key.create(200))
                .build();
        fileInfos.add(fileInfo4);
        FileInfo fileInfo5 = FileInfo.builder()
                .filename("file5")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("right")
                .numberOfRecords(500L)
                .rowKeyTypes(new IntType())
                .minRowKey(Key.create(2))
                .maxRowKey(Key.create(200))
                .build();
        fileInfos.add(fileInfo5);
        FileInfo fileInfo6 = FileInfo.builder()
                .filename("file6")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("right")
                .numberOfRecords(600L)
                .rowKeyTypes(new IntType())
                .minRowKey(Key.create(2))
                .maxRowKey(Key.create(200))
                .build();
        fileInfos.add(fileInfo6);

        // When
        List<CompactionJob> compactionJobs = basicCompactionStrategy.createCompactionJobs(Collections.emptyList(), fileInfos, partitions);

        // Then
        assertThat(compactionJobs).hasSize(3);
        CompactionJob expectedCompactionJob1 = CompactionJob.builder()
                .tableName("table")
                .jobId(compactionJobs.get(0).getId()) // Job id is a UUID so we don't know what it will be
                .partitionId("left")
                .inputFiles(Arrays.asList(fileInfo1.getFilename(), fileInfo2.getFilename()))
                .isSplittingJob(false)
                .outputFile(instanceProperties.get(FILE_SYSTEM) + tableProperties.get(DATA_BUCKET) + "/partition_left/" + compactionJobs.get(0).getId() + ".parquet")
                .childPartitions(null)
                .splitPoint(null)
                .dimension(-1)
                .iteratorClassName(null)
                .iteratorConfig(null).build();
        CompactionJob expectedCompactionJob2 = CompactionJob.builder()
                .tableName("table")
                .jobId(compactionJobs.get(1).getId()) // Job id is a UUID so we don't know what it will be
                .partitionId("left")
                .inputFiles(Arrays.asList(fileInfo3.getFilename(), fileInfo4.getFilename()))
                .isSplittingJob(false)
                .outputFile(instanceProperties.get(FILE_SYSTEM) + tableProperties.get(DATA_BUCKET) + "/partition_left/" + compactionJobs.get(1).getId() + ".parquet")
                .childPartitions(null)
                .splitPoint(null)
                .dimension(-1)
                .iteratorClassName(null)
                .iteratorConfig(null).build();
        CompactionJob expectedCompactionJob3 = CompactionJob.builder()
                .tableName("table")
                .jobId(compactionJobs.get(2).getId()) // Job id is a UUID so we don't know what it will be
                .partitionId("right")
                .inputFiles(Arrays.asList(fileInfo5.getFilename(), fileInfo6.getFilename()))
                .isSplittingJob(false)
                .outputFile(instanceProperties.get(FILE_SYSTEM) + tableProperties.get(DATA_BUCKET) + "/partition_right/" + compactionJobs.get(2).getId() + ".parquet")
                .childPartitions(null)
                .splitPoint(null)
                .dimension(-1)
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
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CONFIG_BUCKET, "bucket");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "table");
        tableProperties.set(DATA_BUCKET, "databucket");
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
        FileInfo fileInfo1 = FileInfo.builder()
                .filename("file1")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("root")
                .numberOfRecords(100L)
                .rowKeyTypes(new IntType())
                .minRowKey(Key.create(1))
                .maxRowKey(Key.create(100))
                .build();
        fileInfos.add(fileInfo1);
        FileInfo fileInfo2 = FileInfo.builder()
                .filename("file2")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("root")
                .numberOfRecords(200L)
                .rowKeyTypes(new IntType())
                .minRowKey(Key.create(2))
                .maxRowKey(Key.create(200))
                .build();
        fileInfos.add(fileInfo2);

        // When
        List<CompactionJob> compactionJobs = basicCompactionStrategy.createCompactionJobs(Collections.emptyList(), fileInfos, partitions);

        // Then
        assertThat(compactionJobs).hasSize(1);
        CompactionJob expectedCompactionJob = CompactionJob.builder()
                .tableName("table")
                .jobId(compactionJobs.get(0).getId()) // Job id is a UUID so we don't know what it will be
                .partitionId("root")
                .inputFiles(Arrays.asList(fileInfo1.getFilename(), fileInfo2.getFilename()))
                .isSplittingJob(true)
                .outputFile(null)
                .outputFiles(new MutablePair<>(
                        compactionJobs.get(0).getOutputFiles().getLeft(),
                        compactionJobs.get(0).getOutputFiles().getRight()))
                .childPartitions(Arrays.asList("left", "right"))
                .splitPoint(10)
                .dimension(0)
                .iteratorClassName(null)
                .iteratorConfig(null).build();
        assertThat(compactionJobs).containsExactly(expectedCompactionJob);
    }
}
