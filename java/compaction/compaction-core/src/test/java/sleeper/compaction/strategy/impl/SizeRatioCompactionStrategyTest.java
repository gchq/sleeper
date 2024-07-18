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
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;

import java.util.ArrayList;
import java.util.List;

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
    private final FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(partitionTree);

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
        List<FileReference> fileReferences = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            FileReference fileReference = fileReferenceFactory.rootFile("file-" + i, i == 7 ? 100L : 50L);
            fileReferences.add(fileReference);
        }
        strategy.init(instanceProperties, tableProperties, fileReferences, partitionTree.getAllPartitions());

        // When
        List<CompactionJob> compactionJobs = strategy.createCompactionJobs();

        // Then
        assertThat(compactionJobs).containsExactly(
                jobWithFiles(compactionJobs.get(0), List.of(
                        "file-0", "file-1", "file-2", "file-3", "file-4", "file-5", "file-6", "file-7")));
    }

    @Test
    public void shouldCreateNoJobsWhenOneLeafPartitionAndFilesDoNotMeetCriteria() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "11");
        SizeRatioCompactionStrategy strategy = new SizeRatioCompactionStrategy();
        List<FileReference> fileReferences = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            FileReference fileReference = fileReferenceFactory.rootFile("file-" + i, (long) Math.pow(2, i + 1));
            fileReferences.add(fileReference);
        }
        strategy.init(instanceProperties, tableProperties, fileReferences, partitionTree.getAllPartitions());

        // When
        List<CompactionJob> compactionJobs = strategy.createCompactionJobs();

        // Then
        assertThat(compactionJobs).isEmpty();
    }

    @Test
    public void shouldCreateMultipleJobsWhenMoreThanBatchFilesMeetCriteria() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "5");
        SizeRatioCompactionStrategy strategy = new SizeRatioCompactionStrategy();
        //  - First batch that meet criteria
        //  - 9, 9, 9, 9, 10
        //  - Second batch that meet criteria
        //  - 90, 90, 90, 90, 100
        //  - Collectively they all meet the criteria as well
        List<FileReference> shuffledFiles = List.of(
                fileReferenceFactory.rootFile("B1", 90),
                fileReferenceFactory.rootFile("A1", 9),
                fileReferenceFactory.rootFile("A2", 9),
                fileReferenceFactory.rootFile("B5", 100),
                fileReferenceFactory.rootFile("B2", 90),
                fileReferenceFactory.rootFile("B3", 90),
                fileReferenceFactory.rootFile("A3", 9),
                fileReferenceFactory.rootFile("A5", 10),
                fileReferenceFactory.rootFile("B4", 90),
                fileReferenceFactory.rootFile("A4", 9));
        strategy.init(instanceProperties, tableProperties, shuffledFiles, partitionTree.getAllPartitions());

        // When
        List<CompactionJob> jobs = strategy.createCompactionJobs();

        // Then
        assertThat(jobs).hasSize(2);
        assertThat(jobs.get(0)).isEqualTo(jobWithFiles(jobs.get(0), List.of("A1", "A2", "A3", "A4", "A5")));
        assertThat(jobs.get(1)).isEqualTo(jobWithFiles(jobs.get(1), List.of("B1", "B2", "B3", "B4", "B5")));
    }

    @Test
    public void shouldCreateJobWithLessThanBatchSizeNumberOfFiles() {
        // Given
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "5");
        tableProperties.set(SIZE_RATIO_COMPACTION_STRATEGY_RATIO, "2");
        SizeRatioCompactionStrategy strategy = new SizeRatioCompactionStrategy();
        //  - First batch that meet criteria
        //  - 9, 9, 9, 9, 10
        //  - Second batch that meet criteria
        //  - 90, 90, 90, 90, 100
        //  - Third batch that meets criteria and is smaller than batch size
        //  - 200, 200, 200
        //  - Collectively they all meet the criteria as well
        List<FileReference> shuffledFiles = List.of(
                fileReferenceFactory.rootFile("B1", 90),
                fileReferenceFactory.rootFile("A1", 9),
                fileReferenceFactory.rootFile("C1", 200),
                fileReferenceFactory.rootFile("A2", 9),
                fileReferenceFactory.rootFile("B5", 100),
                fileReferenceFactory.rootFile("B2", 90),
                fileReferenceFactory.rootFile("B3", 90),
                fileReferenceFactory.rootFile("A3", 9),
                fileReferenceFactory.rootFile("A5", 10),
                fileReferenceFactory.rootFile("C2", 200),
                fileReferenceFactory.rootFile("B4", 90),
                fileReferenceFactory.rootFile("C3", 200),
                fileReferenceFactory.rootFile("A4", 9));
        strategy.init(instanceProperties, tableProperties, shuffledFiles, partitionTree.getAllPartitions());

        // When
        List<CompactionJob> jobs = strategy.createCompactionJobs();

        // Then
        assertThat(jobs).hasSize(3);
        assertThat(jobs.get(0)).isEqualTo(jobWithFiles(jobs.get(0), List.of("A1", "A2", "A3", "A4", "A5")));
        assertThat(jobs.get(1)).isEqualTo(jobWithFiles(jobs.get(1), List.of("B1", "B2", "B3", "B4", "B5")));
        assertThat(jobs.get(2)).isEqualTo(jobWithFiles(jobs.get(2), List.of("C1", "C2", "C3")));
    }

    private CompactionJob jobWithFiles(CompactionJob job, List<String> files) {
        return CompactionJob.builder()
                .tableId("table-id")
                .jobId(job.getId()) // Job id is a UUID so we don't know what it will be
                .partitionId("root")
                .inputFiles(files)
                .outputFile("file://databucket/table-id/data/partition_root/" + job.getId() + ".parquet")
                .iteratorClassName(null)
                .iteratorConfig(null)
                .build();
    }
}
