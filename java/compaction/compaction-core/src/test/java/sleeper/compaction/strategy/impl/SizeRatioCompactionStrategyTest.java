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
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.FileReference;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.SIZE_RATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION;
import static sleeper.configuration.properties.table.TableProperty.SIZE_RATIO_COMPACTION_STRATEGY_RATIO;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class SizeRatioCompactionStrategyTest extends CompactionStrategyTestBase {

    @BeforeEach
    void setUp() {
        strategy = new SizeRatioCompactionStrategy();
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, "databucket");
        tableProperties.set(TABLE_NAME, "table");
        tableProperties.set(TABLE_ID, "table-id");
    }

    @Test
    public void shouldCreateOneJobWhenOneLeafPartitionAndFilesMeetCriteria() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "11");
        List<FileReference> fileReferences = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            FileReference fileReference = fileReferenceFactory.rootFile("file-" + i, i == 7 ? 100L : 50L);
            fileReferences.add(fileReference);
        }
        CompactionJobFactory jobFactory = jobFactoryWithIncrementingJobIds();

        // When
        List<CompactionJob> compactionJobs = createCompactionJobs(jobFactory, fileReferences, partitionTree.getAllPartitions());

        // Then
        assertThat(compactionJobs).containsExactly(
                jobFactory.createCompactionJob("job1", fileReferences, "root"));
    }

    @Test
    public void shouldCreateNoJobsWhenOneLeafPartitionAndFilesDoNotMeetCriteria() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "11");
        List<FileReference> fileReferences = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            FileReference fileReference = fileReferenceFactory.rootFile("file-" + i, (long) Math.pow(2, i + 1));
            fileReferences.add(fileReference);
        }

        // When
        List<CompactionJob> compactionJobs = createCompactionJobs(fileReferences, partitionTree.getAllPartitions());

        // Then
        assertThat(compactionJobs).isEmpty();
    }

    @Test
    public void shouldCreateNoJobsWhenFileInLeafPartitionIsAssignedToAJob() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "5");
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
        List<CompactionJob> compactionJobs = createCompactionJobs(List.of(fileReference), partitionTree.getAllPartitions());

        // Then
        assertThat(compactionJobs).isEmpty();
    }

    @Test
    void shouldCreateOneJobWhenTwoBatchesCanBeCreatedButLimitOfOneJobPerPartitionIsSet() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "5");
        tableProperties.setNumber(SIZE_RATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION, 1);
        List<FileReference> fileReferences = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            FileReference fileReference = fileReferenceFactory.rootFile("file-" + i, i == 7 ? 100L : 50L);
            fileReferences.add(fileReference);
        }
        CompactionJobFactory jobFactory = jobFactoryWithIncrementingJobIds();

        // When
        List<CompactionJob> compactionJobs = createCompactionJobs(jobFactory, fileReferences, partitionTree.getAllPartitions());

        // Then
        assertThat(compactionJobs).containsExactly(
                jobFactory.createCompactionJob("job1", fileReferences.subList(0, 5), "root"));
    }

    @Test
    public void shouldCreateMultipleJobsWhenMoreThanBatchFilesMeetCriteria() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "5");
        //  - First batch that meet criteria
        //  - 9, 9, 9, 9, 10
        //  - Second batch that meet criteria
        //  - 90, 90, 90, 90, 100
        //  - Collectively they all meet the criteria as well
        List<FileReference> firstBatch = List.of(
                fileReferenceFactory.rootFile("A1", 9),
                fileReferenceFactory.rootFile("A2", 9),
                fileReferenceFactory.rootFile("A3", 9),
                fileReferenceFactory.rootFile("A4", 9),
                fileReferenceFactory.rootFile("A5", 10));
        List<FileReference> secondBatch = List.of(
                fileReferenceFactory.rootFile("B1", 90),
                fileReferenceFactory.rootFile("B2", 90),
                fileReferenceFactory.rootFile("B3", 90),
                fileReferenceFactory.rootFile("B4", 90),
                fileReferenceFactory.rootFile("B5", 100));

        List<FileReference> shuffledFiles = List.of(
                secondBatch.get(0),
                firstBatch.get(0),
                firstBatch.get(1),
                secondBatch.get(4),
                secondBatch.get(1),
                secondBatch.get(2),
                firstBatch.get(2),
                firstBatch.get(4),
                secondBatch.get(3),
                firstBatch.get(3));
        CompactionJobFactory jobFactory = jobFactoryWithIncrementingJobIds();

        // When
        List<CompactionJob> jobs = createCompactionJobs(jobFactory, shuffledFiles, partitionTree.getAllPartitions());

        // Then
        assertThat(jobs).containsExactly(
                jobFactory.createCompactionJob("job1", firstBatch, "root"),
                jobFactory.createCompactionJob("job2", secondBatch, "root"));
    }

    @Test
    public void shouldCreateJobWithLessThanBatchSizeNumberOfFiles() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "5");
        tableProperties.set(SIZE_RATIO_COMPACTION_STRATEGY_RATIO, "2");
        //  - First batch that meet criteria
        //  - 9, 9, 9, 9, 10
        //  - Second batch that meet criteria
        //  - 90, 90, 90, 90, 100
        //  - Third batch that meets criteria and is smaller than batch size
        //  - 200, 200, 200
        //  - Collectively they all meet the criteria as well
        List<FileReference> firstBatch = List.of(
                fileReferenceFactory.rootFile("A1", 9),
                fileReferenceFactory.rootFile("A2", 9),
                fileReferenceFactory.rootFile("A3", 9),
                fileReferenceFactory.rootFile("A4", 9),
                fileReferenceFactory.rootFile("A5", 10));
        List<FileReference> secondBatch = List.of(
                fileReferenceFactory.rootFile("B1", 90),
                fileReferenceFactory.rootFile("B2", 90),
                fileReferenceFactory.rootFile("B3", 90),
                fileReferenceFactory.rootFile("B4", 90),
                fileReferenceFactory.rootFile("B5", 100));
        List<FileReference> thirdBatch = List.of(
                fileReferenceFactory.rootFile("C1", 200),
                fileReferenceFactory.rootFile("C2", 200),
                fileReferenceFactory.rootFile("C3", 200));
        List<FileReference> shuffledFiles = List.of(
                secondBatch.get(0),
                firstBatch.get(0),
                thirdBatch.get(0),
                firstBatch.get(1),
                secondBatch.get(4),
                secondBatch.get(1),
                secondBatch.get(2),
                firstBatch.get(2),
                firstBatch.get(4),
                thirdBatch.get(1),
                secondBatch.get(3),
                thirdBatch.get(2),
                firstBatch.get(3));
        CompactionJobFactory jobFactory = jobFactoryWithIncrementingJobIds();

        // When
        List<CompactionJob> jobs = createCompactionJobs(jobFactory, shuffledFiles, partitionTree.getAllPartitions());

        // Then
        assertThat(jobs).containsExactly(
                jobFactory.createCompactionJob("job1", firstBatch, "root"),
                jobFactory.createCompactionJob("job2", secondBatch, "root"),
                jobFactory.createCompactionJob("job3", thirdBatch, "root"));
    }
}
