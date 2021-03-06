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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import sleeper.compaction.job.CompactionJob;
import sleeper.configuration.properties.InstanceProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import sleeper.configuration.properties.table.TableProperties;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.SIZE_RATIO_COMPACTION_STRATEGY_RATIO;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.schema.type.IntType;
import sleeper.statestore.FileInfo;

public class SizeRatioCompactionStrategyTest {

    @Test
    public void shouldCreateOneJobWhenOneLeafPartitionAndFilesMeetCriteria() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CONFIG_BUCKET, "config");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "table");
        tableProperties.set(DATA_BUCKET, "databucket");
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "11");
        SizeRatioCompactionStrategy sizeRatioCompactionStrategy = new SizeRatioCompactionStrategy();
        sizeRatioCompactionStrategy.init(instanceProperties, tableProperties);
        Partition partition = new Partition();
        partition.setId("root");
        partition.setRowKeyTypes(new IntType());
        partition.setLeafPartition(true);
        partition.setParentPartitionId(null);
        partition.setChildPartitionIds(null);
        List<Partition> partitions = Collections.singletonList(partition);
        List<FileInfo> fileInfos = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            FileInfo fileInfo = new FileInfo();
            fileInfo.setFilename("file-" + i);
            fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
            fileInfo.setPartitionId(partition.getId());
            if (i == 7) {
                fileInfo.setNumberOfRecords(100L);
            } else {
                fileInfo.setNumberOfRecords(50L);
            }
            fileInfo.setRowKeyTypes(new IntType());
            fileInfo.setMinRowKey(Key.create(1));
            fileInfo.setMaxRowKey(Key.create(100));
            fileInfos.add(fileInfo);
        }

        // When
        List<CompactionJob> compactionJobs = sizeRatioCompactionStrategy.createCompactionJobs(Collections.EMPTY_LIST, fileInfos, partitions);

        // Then
        assertEquals(1, compactionJobs.size());

        checkJob(compactionJobs.get(0), fileInfos.stream().map(FileInfo::getFilename).collect(Collectors.toList()), partition.getId(), instanceProperties.get(FILE_SYSTEM), tableProperties.get(DATA_BUCKET));
    }
    
    @Test
    public void shouldCreateNoJobsWhenOneLeafPartitionAndFilesDoNotMeetCriteria() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CONFIG_BUCKET, "config");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "table");
        tableProperties.set(DATA_BUCKET, "databucket");
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "11");
        SizeRatioCompactionStrategy sizeRatioCompactionStrategy = new SizeRatioCompactionStrategy();
        sizeRatioCompactionStrategy.init(instanceProperties, tableProperties);
        Partition partition = new Partition();
        partition.setId("root");
        partition.setRowKeyTypes(new IntType());
        partition.setLeafPartition(true);
        partition.setParentPartitionId(null);
        partition.setChildPartitionIds(null);
        List<Partition> partitions = Collections.singletonList(partition);
        List<FileInfo> fileInfos = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            FileInfo fileInfo = new FileInfo();
            fileInfo.setFilename("file-" + i);
            fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
            fileInfo.setPartitionId(partition.getId());
            fileInfo.setNumberOfRecords((long) Math.pow(2, i + 1));
            fileInfo.setRowKeyTypes(new IntType());
            fileInfo.setMinRowKey(Key.create(1));
            fileInfo.setMaxRowKey(Key.create(100));
            fileInfos.add(fileInfo);
        }

        // When
        List<CompactionJob> compactionJobs = sizeRatioCompactionStrategy.createCompactionJobs(Collections.EMPTY_LIST, fileInfos, partitions);

        // Then
        assertEquals(0, compactionJobs.size());
    }
    
    @Test
    public void shouldCreateMultipleJobsWhenMoreThanBatchFilesMeetCriteria() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CONFIG_BUCKET, "config");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "table");
        tableProperties.set(DATA_BUCKET, "databucket");
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "5");
        SizeRatioCompactionStrategy sizeRatioCompactionStrategy = new SizeRatioCompactionStrategy();
        sizeRatioCompactionStrategy.init(instanceProperties, tableProperties);
        Partition partition = new Partition();
        partition.setId("root");
        partition.setRowKeyTypes(new IntType());
        partition.setLeafPartition(true);
        partition.setParentPartitionId(null);
        partition.setChildPartitionIds(null);
        List<Partition> partitions = Collections.singletonList(partition);
        //  - First batch that meet criteria
        //  - 9, 9, 9, 9, 10
        //  - Second batch that meet criteria
        //  - 90, 90, 90, 90, 100
        //  - Collectively they all meet the criteria as well
        List<Integer> sizes = Arrays.asList(9, 9, 9, 9, 10, 90, 90, 90, 90, 100);
        List<FileInfo> fileInfos = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            FileInfo fileInfo = new FileInfo();
            fileInfo.setFilename("file-" + i);
            fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
            fileInfo.setPartitionId(partition.getId());
            fileInfo.setNumberOfRecords((long) sizes.get(i));
            fileInfo.setRowKeyTypes(new IntType());
            fileInfo.setMinRowKey(Key.create(1));
            fileInfo.setMaxRowKey(Key.create(100));
            fileInfos.add(fileInfo);
        }
        List<FileInfo> shuffledFileInfos = new ArrayList<>(fileInfos);
        Collections.shuffle(shuffledFileInfos);

        // When
        List<CompactionJob> compactionJobs = sizeRatioCompactionStrategy.createCompactionJobs(Collections.EMPTY_LIST, shuffledFileInfos, partitions);

        // Then
        assertEquals(2, compactionJobs.size());
        
        List<String> filesForJob1 = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            filesForJob1.add(fileInfos.get(i).getFilename());
        }
        checkJob(compactionJobs.get(0), filesForJob1, partition.getId(), instanceProperties.get(FILE_SYSTEM), tableProperties.get(DATA_BUCKET));
        
        List<String> filesForJob2 = new ArrayList<>();
        for (int i = 5; i < 10; i++) {
            filesForJob2.add(fileInfos.get(i).getFilename());
        }
        checkJob(compactionJobs.get(1), filesForJob2, partition.getId(), instanceProperties.get(FILE_SYSTEM), tableProperties.get(DATA_BUCKET));
    }
    
    @Test
    public void shouldCreateJobWithLessThanBatchSizeNumberOfFiles() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CONFIG_BUCKET, "config");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "table");
        tableProperties.set(DATA_BUCKET, "databucket");
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "5");
        tableProperties.set(SIZE_RATIO_COMPACTION_STRATEGY_RATIO, "2");
        SizeRatioCompactionStrategy sizeRatioCompactionStrategy = new SizeRatioCompactionStrategy();
        sizeRatioCompactionStrategy.init(instanceProperties, tableProperties);
        Partition partition = new Partition();
        partition.setId("root");
        partition.setRowKeyTypes(new IntType());
        partition.setLeafPartition(true);
        partition.setParentPartitionId(null);
        partition.setChildPartitionIds(null);
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
            FileInfo fileInfo = new FileInfo();
            fileInfo.setFilename("file-" + i);
            fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
            fileInfo.setPartitionId(partition.getId());
            fileInfo.setNumberOfRecords((long) sizes.get(i));
            fileInfo.setRowKeyTypes(new IntType());
            fileInfo.setMinRowKey(Key.create(1));
            fileInfo.setMaxRowKey(Key.create(100));
            fileInfos.add(fileInfo);
        }
        List<FileInfo> shuffledFileInfos = new ArrayList<>(fileInfos);
        Collections.shuffle(shuffledFileInfos);

        // When
        List<CompactionJob> compactionJobs = sizeRatioCompactionStrategy.createCompactionJobs(Collections.EMPTY_LIST, shuffledFileInfos, partitions);

        // Then
        assertEquals(3, compactionJobs.size());
        
        List<String> filesForJob1 = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            filesForJob1.add(fileInfos.get(i).getFilename());
        }
        checkJob(compactionJobs.get(0), filesForJob1, partition.getId(), instanceProperties.get(FILE_SYSTEM), tableProperties.get(DATA_BUCKET));
        
        List<String> filesForJob2 = new ArrayList<>();
        for (int i = 5; i < 10; i++) {
            filesForJob2.add(fileInfos.get(i).getFilename());
        }
        checkJob(compactionJobs.get(1), filesForJob2, partition.getId(), instanceProperties.get(FILE_SYSTEM), tableProperties.get(DATA_BUCKET));
        
        List<String> filesForJob3 = new ArrayList<>();
        for (int i = 10; i < 13; i++) {
            filesForJob3.add(fileInfos.get(i).getFilename());
        }
        checkJob(compactionJobs.get(2), filesForJob3, partition.getId(), instanceProperties.get(FILE_SYSTEM), tableProperties.get(DATA_BUCKET));
    }
    
    private void checkJob(CompactionJob job, List<String> files, String partitionId, String fs, String bucket) {
        CompactionJob expectedCompactionJob = new CompactionJob("table", job.getId()); // Job id is a UUID so we don't know what it will be
        expectedCompactionJob.setPartitionId(partitionId);
        List<String> filesForJob = new ArrayList<>();
        for (String file : files) {
            filesForJob.add(file);
        }
        expectedCompactionJob.setInputFiles(filesForJob);
        job.getInputFiles().sort(Comparator.naturalOrder());
        expectedCompactionJob.setIsSplittingJob(false);
        expectedCompactionJob.setOutputFile(fs + bucket + "/partition_" + partitionId + "/" + job.getId() + ".parquet");
        expectedCompactionJob.setChildPartitions(null);
        expectedCompactionJob.setSplitPoint(null);
        expectedCompactionJob.setDimension(-1);
        expectedCompactionJob.setIteratorClassName(null);
        expectedCompactionJob.setIteratorConfig(null);
        assertEquals(expectedCompactionJob, job);
    }
}