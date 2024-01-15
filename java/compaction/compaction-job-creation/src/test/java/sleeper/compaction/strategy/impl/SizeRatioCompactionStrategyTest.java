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
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.SIZE_RATIO_COMPACTION_STRATEGY_RATIO;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class SizeRatioCompactionStrategyTest {

    private static final Schema DEFAULT_SCHEMA = schemaWithKey("key");
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, DEFAULT_SCHEMA);
    private final PartitionTree partitionTree = new PartitionsBuilder(DEFAULT_SCHEMA)
            .singlePartition("root")
            .buildTree();
    private final FileInfoFactory fileInfoFactory = FileInfoFactory.from(partitionTree);

    @BeforeEach
    void setUp() {
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, "databucket");
        tableProperties.set(TABLE_NAME, "table");
        tableProperties.set(TABLE_ID, "table-id");
    }

    @Test
    public void shouldCreateOneJobWhenOneLeafPartitionAndFilesMeetCriteria() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "11");
        SizeRatioCompactionStrategy strategy = new SizeRatioCompactionStrategy();
        strategy.init(instanceProperties, tableProperties);
        List<FileInfo> fileInfos = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            FileInfo fileInfo = fileInfoFactory.rootFile("file-" + i, i == 7 ? 100L : 50L);
            fileInfos.add(fileInfo);
        }

        // When
        List<CompactionJob> compactionJobs = strategy.createCompactionJobs(List.of(), fileInfos, partitionTree.getAllPartitions());

        // Then
        assertThat(compactionJobs).hasSize(1);
        checkJob(compactionJobs.get(0), fileInfos);
    }

    @Test
    public void shouldCreateNoJobsWhenOneLeafPartitionAndFilesDoNotMeetCriteria() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "11");
        SizeRatioCompactionStrategy strategy = new SizeRatioCompactionStrategy();
        strategy.init(instanceProperties, tableProperties);
        List<FileInfo> fileInfos = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            FileInfo fileInfo = fileInfoFactory.rootFile("file-" + i, (long) Math.pow(2, i + 1));
            fileInfos.add(fileInfo);
        }

        // When
        List<CompactionJob> compactionJobs = strategy.createCompactionJobs(List.of(), fileInfos, partitionTree.getAllPartitions());

        // Then
        assertThat(compactionJobs).isEmpty();
    }

    @Test
    public void shouldCreateMultipleJobsWhenMoreThanBatchFilesMeetCriteria() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "5");
        SizeRatioCompactionStrategy strategy = new SizeRatioCompactionStrategy();
        strategy.init(instanceProperties, tableProperties);
        //  - First batch that meet criteria
        //  - 9, 9, 9, 9, 10
        //  - Second batch that meet criteria
        //  - 90, 90, 90, 90, 100
        //  - Collectively they all meet the criteria as well
        List<Integer> sizes = Arrays.asList(9, 9, 9, 9, 10, 90, 90, 90, 90, 100);
        List<FileInfo> fileInfos = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            FileInfo fileInfo = fileInfoFactory.rootFile("file-" + i, (long) sizes.get(i));
            fileInfos.add(fileInfo);
        }
        List<FileInfo> shuffledFileInfos = new ArrayList<>(fileInfos);
        Collections.shuffle(shuffledFileInfos);

        // When
        List<CompactionJob> compactionJobs = strategy.createCompactionJobs(List.of(), shuffledFileInfos, partitionTree.getAllPartitions());

        // Then
        assertThat(compactionJobs).hasSize(2);
        checkJob(compactionJobs.get(0), fileInfos.subList(0, 5));
        checkJob(compactionJobs.get(1), fileInfos.subList(5, 10));
    }

    @Test
    public void shouldCreateJobWithLessThanBatchSizeNumberOfFiles() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "5");
        tableProperties.set(SIZE_RATIO_COMPACTION_STRATEGY_RATIO, "2");
        SizeRatioCompactionStrategy strategy = new SizeRatioCompactionStrategy();
        strategy.init(instanceProperties, tableProperties);
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
            FileInfo fileInfo = fileInfoFactory.rootFile("file-" + i, (long) sizes.get(i));
            fileInfos.add(fileInfo);
        }
        List<FileInfo> shuffledFileInfos = new ArrayList<>(fileInfos);
        Collections.shuffle(shuffledFileInfos);

        // When
        List<CompactionJob> compactionJobs = strategy.createCompactionJobs(List.of(), shuffledFileInfos, partitionTree.getAllPartitions());

        // Then
        assertThat(compactionJobs).hasSize(3);
        checkJob(compactionJobs.get(0), fileInfos.subList(0, 5));
        checkJob(compactionJobs.get(1), fileInfos.subList(5, 10));
        checkJob(compactionJobs.get(2), fileInfos.subList(10, 13));
    }

    private void checkJob(CompactionJob job, List<FileInfo> files) {
        CompactionJob expectedCompactionJob = CompactionJob.builder()
                .tableId("table-id")
                .jobId(job.getId()) // Job id is a UUID so we don't know what it will be
                .partitionId("root")
                .inputFiles(files.stream().map(FileInfo::getFilename).collect(Collectors.toList()))
                .isSplittingJob(false)
                .outputFile("file://databucket/table-id/partition_root/" + job.getId() + ".parquet")
                .iteratorClassName(null)
                .iteratorConfig(null).build();
        job.getInputFiles().sort(Comparator.naturalOrder());
        assertThat(job).isEqualTo(expectedCompactionJob);
    }
}
