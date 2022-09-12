/*
 * Copyright 2022 Crown Copyright
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
import org.junit.Test;
import sleeper.compaction.job.CompactionJob;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.statestore.FileInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
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
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setFilename("file1");
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setPartitionId(partition.getId());
        fileInfo1.setNumberOfRecords(100L);
        fileInfo1.setRowKeyTypes(new IntType());
        fileInfo1.setMinRowKey(Key.create(1));
        fileInfo1.setMaxRowKey(Key.create(100));
        fileInfos.add(fileInfo1);
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setFilename("file2");
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setPartitionId(partition.getId());
        fileInfo2.setNumberOfRecords(100L);
        fileInfo2.setRowKeyTypes(new IntType());
        fileInfo2.setMinRowKey(Key.create(2));
        fileInfo2.setMaxRowKey(Key.create(200));
        fileInfos.add(fileInfo2);

        // When
        List<CompactionJob> compactionJobs = basicCompactionStrategy.createCompactionJobs(Collections.emptyList(), fileInfos, partitions);

        // Then
        assertThat(compactionJobs).hasSize(1);
        CompactionJob expectedCompactionJob = new CompactionJob("table", compactionJobs.get(0).getId()); // Job id is a UUID so we don't know what it will be
        expectedCompactionJob.setPartitionId(partition.getId());
        expectedCompactionJob.setInputFiles(Arrays.asList(fileInfo1.getFilename(), fileInfo2.getFilename()));
        expectedCompactionJob.setIsSplittingJob(false);
        expectedCompactionJob.setOutputFile(instanceProperties.get(FILE_SYSTEM) + tableProperties.get(DATA_BUCKET) + "/partition_" + partition.getId() + "/" + compactionJobs.get(0).getId() + ".parquet");
        expectedCompactionJob.setChildPartitions(null);
        expectedCompactionJob.setSplitPoint(null);
        expectedCompactionJob.setDimension(-1);
        expectedCompactionJob.setIteratorClassName(null);
        expectedCompactionJob.setIteratorConfig(null);
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
            FileInfo fileInfo = new FileInfo();
            fileInfo.setFilename("file-" + i);
            fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
            fileInfo.setPartitionId(partition.getId());
            fileInfo.setNumberOfRecords(1_000_000L - i * 100L); // Files provided in decreasing order of size - the jobs should consider them in increasing order so that files of similar sizes are processed together
            fileInfo.setRowKeyTypes(new IntType());
            fileInfo.setMinRowKey(Key.create(1));
            fileInfo.setMaxRowKey(Key.create(100));
            fileInfos.add(fileInfo);
        }

        // When
        List<CompactionJob> compactionJobs = basicCompactionStrategy.createCompactionJobs(Collections.emptyList(), fileInfos, partitions);

        // Then
        assertThat(compactionJobs).hasSize(10).isEqualTo(IntStream.range(0, 10).mapToObj(i -> {
            CompactionJob expectedCompactionJob = new CompactionJob("table", compactionJobs.get(i).getId()); // Job id is a UUID so we don't know what it will be
            expectedCompactionJob.setPartitionId(partition.getId());
            List<String> inputFiles = new ArrayList<>();
            for (int j = 99 - i * 10; j > 99 - (i + 1) * 10; j--) {
                inputFiles.add("file-" + j);
            }
            expectedCompactionJob.setInputFiles(inputFiles);
            expectedCompactionJob.setIsSplittingJob(false);
            expectedCompactionJob.setOutputFile(instanceProperties.get(FILE_SYSTEM) + tableProperties.get(DATA_BUCKET) + "/partition_" + partition.getId() + "/" + compactionJobs.get(i).getId() + ".parquet");
            expectedCompactionJob.setChildPartitions(null);
            expectedCompactionJob.setSplitPoint(null);
            expectedCompactionJob.setDimension(-1);
            expectedCompactionJob.setIteratorClassName(null);
            expectedCompactionJob.setIteratorConfig(null);
            return expectedCompactionJob;
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
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setFilename("file1");
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setPartitionId(partition.getId());
        fileInfo1.setNumberOfRecords(100L);
        fileInfo1.setRowKeyTypes(new IntType());
        fileInfo1.setMinRowKey(Key.create(1));
        fileInfo1.setMaxRowKey(Key.create(100));
        fileInfos.add(fileInfo1);
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setFilename("file2");
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setPartitionId(partition.getId());
        fileInfo2.setNumberOfRecords(100L);
        fileInfo2.setRowKeyTypes(new IntType());
        fileInfo2.setMinRowKey(Key.create(2));
        fileInfo2.setMaxRowKey(Key.create(200));
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
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setFilename("file1");
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setPartitionId("left");
        fileInfo1.setNumberOfRecords(100L);
        fileInfo1.setRowKeyTypes(new IntType());
        fileInfo1.setMinRowKey(Key.create(1));
        fileInfo1.setMaxRowKey(Key.create(100));
        fileInfos.add(fileInfo1);
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setFilename("file2");
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setPartitionId("left");
        fileInfo2.setNumberOfRecords(200L);
        fileInfo2.setRowKeyTypes(new IntType());
        fileInfo2.setMinRowKey(Key.create(2));
        fileInfo2.setMaxRowKey(Key.create(200));
        fileInfos.add(fileInfo2);
        FileInfo fileInfo3 = new FileInfo();
        fileInfo3.setFilename("file3");
        fileInfo3.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo3.setPartitionId("left");
        fileInfo3.setNumberOfRecords(300L);
        fileInfo3.setRowKeyTypes(new IntType());
        fileInfo3.setMinRowKey(Key.create(1));
        fileInfo3.setMaxRowKey(Key.create(100));
        fileInfos.add(fileInfo3);
        FileInfo fileInfo4 = new FileInfo();
        fileInfo4.setFilename("file4");
        fileInfo4.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo4.setPartitionId("left");
        fileInfo4.setNumberOfRecords(400L);
        fileInfo4.setRowKeyTypes(new IntType());
        fileInfo4.setMinRowKey(Key.create(2));
        fileInfo4.setMaxRowKey(Key.create(200));
        fileInfos.add(fileInfo4);
        FileInfo fileInfo5 = new FileInfo();
        fileInfo5.setFilename("file5");
        fileInfo5.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo5.setPartitionId("right");
        fileInfo5.setNumberOfRecords(500L);
        fileInfo5.setRowKeyTypes(new IntType());
        fileInfo5.setMinRowKey(Key.create(2));
        fileInfo5.setMaxRowKey(Key.create(200));
        fileInfos.add(fileInfo5);
        FileInfo fileInfo6 = new FileInfo();
        fileInfo6.setFilename("file6");
        fileInfo6.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo6.setPartitionId("right");
        fileInfo6.setNumberOfRecords(600L);
        fileInfo6.setRowKeyTypes(new IntType());
        fileInfo6.setMinRowKey(Key.create(2));
        fileInfo6.setMaxRowKey(Key.create(200));
        fileInfos.add(fileInfo6);

        // When
        List<CompactionJob> compactionJobs = basicCompactionStrategy.createCompactionJobs(Collections.emptyList(), fileInfos, partitions);

        // Then
        assertThat(compactionJobs).hasSize(3);
        CompactionJob expectedCompactionJob1 = new CompactionJob("table", compactionJobs.get(0).getId()); // Job id is a UUID so we don't know what it will be
        expectedCompactionJob1.setPartitionId("left");
        expectedCompactionJob1.setInputFiles(Arrays.asList(fileInfo1.getFilename(), fileInfo2.getFilename()));
        expectedCompactionJob1.setIsSplittingJob(false);
        expectedCompactionJob1.setOutputFile(instanceProperties.get(FILE_SYSTEM) + tableProperties.get(DATA_BUCKET) + "/partition_left/" + compactionJobs.get(0).getId() + ".parquet");
        expectedCompactionJob1.setChildPartitions(null);
        expectedCompactionJob1.setSplitPoint(null);
        expectedCompactionJob1.setDimension(-1);
        expectedCompactionJob1.setIteratorClassName(null);
        expectedCompactionJob1.setIteratorConfig(null);
        CompactionJob expectedCompactionJob2 = new CompactionJob("table", compactionJobs.get(1).getId()); // Job id is a UUID so we don't know what it will be
        expectedCompactionJob2.setPartitionId("left");
        expectedCompactionJob2.setInputFiles(Arrays.asList(fileInfo3.getFilename(), fileInfo4.getFilename()));
        expectedCompactionJob2.setIsSplittingJob(false);
        expectedCompactionJob2.setOutputFile(instanceProperties.get(FILE_SYSTEM) + tableProperties.get(DATA_BUCKET) + "/partition_left/" + compactionJobs.get(1).getId() + ".parquet");
        expectedCompactionJob2.setChildPartitions(null);
        expectedCompactionJob2.setSplitPoint(null);
        expectedCompactionJob2.setDimension(-1);
        expectedCompactionJob2.setIteratorClassName(null);
        expectedCompactionJob2.setIteratorConfig(null);
        CompactionJob expectedCompactionJob3 = new CompactionJob("table", compactionJobs.get(2).getId()); // Job id is a UUID so we don't know what it will be
        expectedCompactionJob3.setPartitionId("right");
        expectedCompactionJob3.setInputFiles(Arrays.asList(fileInfo5.getFilename(), fileInfo6.getFilename()));
        expectedCompactionJob3.setIsSplittingJob(false);
        expectedCompactionJob3.setOutputFile(instanceProperties.get(FILE_SYSTEM) + tableProperties.get(DATA_BUCKET) + "/partition_right/" + compactionJobs.get(2).getId() + ".parquet");
        expectedCompactionJob3.setChildPartitions(null);
        expectedCompactionJob3.setSplitPoint(null);
        expectedCompactionJob3.setDimension(-1);
        expectedCompactionJob3.setIteratorClassName(null);
        expectedCompactionJob3.setIteratorConfig(null);
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
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setFilename("file1");
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setPartitionId("root");
        fileInfo1.setNumberOfRecords(100L);
        fileInfo1.setRowKeyTypes(new IntType());
        fileInfo1.setMinRowKey(Key.create(1));
        fileInfo1.setMaxRowKey(Key.create(100));
        fileInfos.add(fileInfo1);
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setFilename("file2");
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setPartitionId("root");
        fileInfo2.setNumberOfRecords(200L);
        fileInfo2.setRowKeyTypes(new IntType());
        fileInfo2.setMinRowKey(Key.create(2));
        fileInfo2.setMaxRowKey(Key.create(200));
        fileInfos.add(fileInfo2);

        // When
        List<CompactionJob> compactionJobs = basicCompactionStrategy.createCompactionJobs(Collections.emptyList(), fileInfos, partitions);

        // Then
        assertThat(compactionJobs).hasSize(1);
        CompactionJob expectedCompactionJob = new CompactionJob("table", compactionJobs.get(0).getId()); // Job id is a UUID so we don't know what it will be
        expectedCompactionJob.setPartitionId("root");
        expectedCompactionJob.setInputFiles(Arrays.asList(fileInfo1.getFilename(), fileInfo2.getFilename()));
        expectedCompactionJob.setIsSplittingJob(true);
        expectedCompactionJob.setOutputFile(null);
        expectedCompactionJob.setOutputFiles(new MutablePair<>(
                compactionJobs.get(0).getOutputFiles().getLeft(),
                compactionJobs.get(0).getOutputFiles().getRight()));
        expectedCompactionJob.setChildPartitions(Arrays.asList("left", "right"));
        expectedCompactionJob.setSplitPoint(10);
        expectedCompactionJob.setDimension(0);
        expectedCompactionJob.setIteratorClassName(null);
        expectedCompactionJob.setIteratorConfig(null);
        assertThat(compactionJobs).containsExactly(expectedCompactionJob);
    }
}
