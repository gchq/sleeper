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

import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.schema.type.IntType;
import sleeper.core.statestore.FileInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.SIZE_RATIO_COMPACTION_STRATEGY_RATIO;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class SizeRatioCompactionStrategyTest {

    @Test
    public void shouldCreateOneJobWhenOneLeafPartitionAndFilesMeetCriteria() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CONFIG_BUCKET, "config");
        instanceProperties.set(DATA_BUCKET, "databucket");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "table");
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "11");
        SizeRatioCompactionStrategy sizeRatioCompactionStrategy = new SizeRatioCompactionStrategy();
        sizeRatioCompactionStrategy.init(instanceProperties, tableProperties);
        Partition partition = Partition.builder()
                .id("root")
                .rowKeyTypes(new IntType())
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(Collections.emptyList())
                .build();
        List<Partition> partitions = Collections.singletonList(partition);
        List<FileInfo> fileInfos = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .filename("file-" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId(partition.getId())
                    .numberOfRecords(i == 7 ? 100L : 50L)
                    .rowKeyTypes(new IntType())
                    .minRowKey(Key.create(1))
                    .maxRowKey(Key.create(100))
                    .build();
            fileInfos.add(fileInfo);
        }

        // When
        List<CompactionJob> compactionJobs = sizeRatioCompactionStrategy.createCompactionJobs(Collections.emptyList(), fileInfos, partitions);

        // Then
        assertThat(compactionJobs).hasSize(1);

        checkJob(compactionJobs.get(0), fileInfos.stream().map(FileInfo::getFilename).collect(Collectors.toList()), partition.getId(), instanceProperties.get(FILE_SYSTEM));
    }

    @Test
    public void shouldCreateNoJobsWhenOneLeafPartitionAndFilesDoNotMeetCriteria() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CONFIG_BUCKET, "config");
        instanceProperties.set(DATA_BUCKET, "databucket");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "table");
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "11");
        SizeRatioCompactionStrategy sizeRatioCompactionStrategy = new SizeRatioCompactionStrategy();
        sizeRatioCompactionStrategy.init(instanceProperties, tableProperties);
        Partition partition = Partition.builder()
                .id("root")
                .rowKeyTypes(new IntType())
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(Collections.emptyList())
                .build();
        List<Partition> partitions = Collections.singletonList(partition);
        List<FileInfo> fileInfos = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .filename("file-" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId(partition.getId())
                    .numberOfRecords((long) Math.pow(2, i + 1))
                    .rowKeyTypes(new IntType())
                    .minRowKey(Key.create(1))
                    .maxRowKey(Key.create(100))
                    .build();
            fileInfos.add(fileInfo);
        }

        // When
        List<CompactionJob> compactionJobs = sizeRatioCompactionStrategy.createCompactionJobs(Collections.emptyList(), fileInfos, partitions);

        // Then
        assertThat(compactionJobs).isEmpty();
    }

    @Test
    public void shouldCreateMultipleJobsWhenMoreThanBatchFilesMeetCriteria() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CONFIG_BUCKET, "config");
        instanceProperties.set(DATA_BUCKET, "databucket");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "table");
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "5");
        SizeRatioCompactionStrategy sizeRatioCompactionStrategy = new SizeRatioCompactionStrategy();
        sizeRatioCompactionStrategy.init(instanceProperties, tableProperties);
        Partition partition = Partition.builder()
                .id("root")
                .rowKeyTypes(new IntType())
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(Collections.emptyList())
                .build();
        List<Partition> partitions = Collections.singletonList(partition);
        //  - First batch that meet criteria
        //  - 9, 9, 9, 9, 10
        //  - Second batch that meet criteria
        //  - 90, 90, 90, 90, 100
        //  - Collectively they all meet the criteria as well
        List<Integer> sizes = Arrays.asList(9, 9, 9, 9, 10, 90, 90, 90, 90, 100);
        List<FileInfo> fileInfos = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .filename("file-" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId(partition.getId())
                    .numberOfRecords((long) sizes.get(i))
                    .rowKeyTypes(new IntType())
                    .minRowKey(Key.create(1))
                    .maxRowKey(Key.create(100))
                    .build();
            fileInfos.add(fileInfo);
        }
        List<FileInfo> shuffledFileInfos = new ArrayList<>(fileInfos);
        Collections.shuffle(shuffledFileInfos);

        // When
        List<CompactionJob> compactionJobs = sizeRatioCompactionStrategy.createCompactionJobs(Collections.emptyList(), shuffledFileInfos, partitions);

        // Then
        assertThat(compactionJobs).hasSize(2);

        List<String> filesForJob1 = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            filesForJob1.add(fileInfos.get(i).getFilename());
        }
        checkJob(compactionJobs.get(0), filesForJob1, partition.getId(), instanceProperties.get(FILE_SYSTEM));

        List<String> filesForJob2 = new ArrayList<>();
        for (int i = 5; i < 10; i++) {
            filesForJob2.add(fileInfos.get(i).getFilename());
        }
        checkJob(compactionJobs.get(1), filesForJob2, partition.getId(), instanceProperties.get(FILE_SYSTEM));
    }

    @Test
    public void shouldCreateJobWithLessThanBatchSizeNumberOfFiles() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CONFIG_BUCKET, "config");
        instanceProperties.set(DATA_BUCKET, "databucket");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "table");
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "5");
        tableProperties.set(SIZE_RATIO_COMPACTION_STRATEGY_RATIO, "2");
        SizeRatioCompactionStrategy sizeRatioCompactionStrategy = new SizeRatioCompactionStrategy();
        sizeRatioCompactionStrategy.init(instanceProperties, tableProperties);
        Partition partition = Partition.builder()
                .id("root")
                .rowKeyTypes(new IntType())
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(Collections.emptyList())
                .build();
        List<Partition> partitions = Collections.singletonList(partition);
        //  - First batch that meet criteria
        //  - 9, 9, 9, 9, 10
        //  - Second batch that meet criteria
        //  - 90, 90, 90, 90, 100
        //  - Third batch that meets criteria and is smaller than batch size
        //  - 200, 200, 200
        //  - Collectively they all meet the criteria as well
        List<Integer> sizes = Arrays.asList(9, 9, 9, 9, 10, 90, 90, 90, 90, 100, 200, 200, 200);
        List<FileInfo> fileInfos = new ArrayList<>();
        for (int i = 0; i < sizes.size(); i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .filename("file-" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId(partition.getId())
                    .numberOfRecords((long) sizes.get(i))
                    .rowKeyTypes(new IntType())
                    .minRowKey(Key.create(1))
                    .maxRowKey(Key.create(100))
                    .build();
            fileInfos.add(fileInfo);
        }
        List<FileInfo> shuffledFileInfos = new ArrayList<>(fileInfos);
        Collections.shuffle(shuffledFileInfos);

        // When
        List<CompactionJob> compactionJobs = sizeRatioCompactionStrategy.createCompactionJobs(Collections.emptyList(), shuffledFileInfos, partitions);

        // Then
        assertThat(compactionJobs).hasSize(3);

        List<String> filesForJob1 = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            filesForJob1.add(fileInfos.get(i).getFilename());
        }
        checkJob(compactionJobs.get(0), filesForJob1, partition.getId(), instanceProperties.get(FILE_SYSTEM));

        List<String> filesForJob2 = new ArrayList<>();
        for (int i = 5; i < 10; i++) {
            filesForJob2.add(fileInfos.get(i).getFilename());
        }
        checkJob(compactionJobs.get(1), filesForJob2, partition.getId(), instanceProperties.get(FILE_SYSTEM));

        List<String> filesForJob3 = new ArrayList<>();
        for (int i = 10; i < 13; i++) {
            filesForJob3.add(fileInfos.get(i).getFilename());
        }
        checkJob(compactionJobs.get(2), filesForJob3, partition.getId(), instanceProperties.get(FILE_SYSTEM));
    }

    private void checkJob(CompactionJob job, List<String> files, String partitionId, String fileSystem) {
        CompactionJob expectedCompactionJob = CompactionJob.builder()
                .tableName("table")
                .jobId(job.getId()) // Job id is a UUID so we don't know what it will be
                .partitionId(partitionId)
                .inputFiles(new ArrayList<>(files))
                .isSplittingJob(false)
                .outputFile(fileSystem + "databucket/table/partition_" + partitionId + "/" + job.getId() + ".parquet")
                .childPartitions(null)
                .splitPoint(null)
                .dimension(-1)
                .iteratorClassName(null)
                .iteratorConfig(null).build();
        job.getInputFiles().sort(Comparator.naturalOrder());
        assertThat(job).isEqualTo(expectedCompactionJob);
    }
}
