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

package sleeper.systemtest.dsl.compaction;

import org.approvaltests.Approvals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.creation.strategy.impl.BasicCompactionStrategy;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.sourcedata.RecordNumbers;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;

import java.nio.file.Path;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.core.properties.table.TableProperty.INGEST_FILE_WRITING_STRATEGY;
import static sleeper.core.properties.validation.IngestFileWritingStrategy.ONE_FILE_PER_LEAF;
import static sleeper.core.testutils.printers.FileReferencePrinter.printFiles;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.IN_MEMORY_MAIN;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;
import static sleeper.systemtest.dsl.util.SystemTestSchema.ROW_KEY_FIELD_NAME;

@InMemoryDslTest
public class CompactionTest {
    private final Path tempDir = null;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) throws Exception {
        sleeper.connectToInstance(IN_MEMORY_MAIN);
    }

    @Nested
    @DisplayName("Merge whole files together")
    class MergeFiles {

        @Test
        void shouldCompactFilesUsingDefaultCompactionStrategy(SleeperSystemTest sleeper) {
            // Given
            sleeper.updateTableProperties(Map.of(
                    COMPACTION_FILES_BATCH_SIZE, "5"));
            // Files with records 9, 9, 9, 9, 10 (which match SizeRatioStrategy criteria)
            RecordNumbers numbers = sleeper.scrambleNumberedRecords(LongStream.range(0, 46));
            sleeper.ingest().direct(tempDir)
                    .numberedRecords(numbers.range(0, 9))
                    .numberedRecords(numbers.range(9, 18))
                    .numberedRecords(numbers.range(18, 27))
                    .numberedRecords(numbers.range(27, 36))
                    .numberedRecords(numbers.range(36, 46));

            // When
            sleeper.compaction().createJobs(1).waitForTasks(1).waitForJobs();

            // Then
            assertThat(sleeper.directQuery().allRecordsInTable())
                    .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 46)));
            Approvals.verify(printFiles(sleeper.partitioning().tree(), sleeper.tableFiles().all()));
        }

        @Test
        void shouldCompactFilesUsingBasicCompactionStrategy(SleeperSystemTest sleeper) {
            // Given
            sleeper.updateTableProperties(Map.of(
                    COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                    COMPACTION_FILES_BATCH_SIZE, "2"));
            RecordNumbers numbers = sleeper.scrambleNumberedRecords(LongStream.range(0, 100));
            sleeper.ingest().direct(tempDir)
                    .numberedRecords(numbers.range(0, 25))
                    .numberedRecords(numbers.range(25, 50))
                    .numberedRecords(numbers.range(50, 75))
                    .numberedRecords(numbers.range(75, 100));

            // When
            sleeper.compaction().createJobs(2).waitForTasks(1).waitForJobs();

            // Then
            assertThat(sleeper.directQuery().allRecordsInTable())
                    .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 100)));
            Approvals.verify(printFiles(sleeper.partitioning().tree(), sleeper.tableFiles().all()));
        }
    }

    @Nested
    @DisplayName("Merge parts of files referenced on multiple partitions")
    class MergePartialFiles {

        @BeforeEach
        void setUp(SleeperSystemTest sleeper) {
            sleeper.setGeneratorOverrides(overrideField(
                    ROW_KEY_FIELD_NAME, numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
            sleeper.partitioning().setPartitions(new PartitionsBuilder(DEFAULT_SCHEMA)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "row-50")
                    .splitToNewChildren("L", "LL", "LR", "row-25")
                    .splitToNewChildren("R", "RL", "RR", "row-75")
                    .buildTree());
        }

        @Test
        void shouldCompactOneFileIntoExistingFilesOnLeafPartitions(SleeperSystemTest sleeper) throws Exception {
            // Given a compaction strategy which will always compact two files together
            sleeper.updateTableProperties(Map.of(
                    COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                    COMPACTION_FILES_BATCH_SIZE, "2"));
            // A file which we add to all 4 leaf partitions
            sleeper.sourceFiles().inDataBucket().writeSketches()
                    .createWithNumberedRecords("file.parquet", LongStream.range(0, 50).map(n -> n * 2));
            sleeper.ingest().toStateStore().addFileWithRecordEstimatesOnPartitions(
                    "file.parquet", Map.of(
                            "LL", 12L,
                            "LR", 12L,
                            "RL", 12L,
                            "RR", 12L));
            // And a file in each leaf partition
            sleeper.updateTableProperties(Map.of(INGEST_FILE_WRITING_STRATEGY, ONE_FILE_PER_LEAF.toString()));
            sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 50).map(n -> n * 2 + 1));

            // When we run compaction
            sleeper.compaction().createJobs(4).waitForTasks(1).waitForJobs();

            // Then the same records should be present, in one file on each leaf partition
            assertThat(sleeper.directQuery().allRecordsInTable())
                    .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 100)));
            Approvals.verify(printFiles(sleeper.partitioning().tree(), sleeper.tableFiles().all()));
        }

        @Test
        void shouldCompactOneFileFromRootIntoExistingFilesOnLeafPartitions(SleeperSystemTest sleeper) throws Exception {
            // Given a compaction strategy which will always compact two files together
            sleeper.updateTableProperties(Map.of(
                    COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                    COMPACTION_FILES_BATCH_SIZE, "2"));
            // And a file which we add to the root partition
            sleeper.sourceFiles().inDataBucket().writeSketches()
                    .createWithNumberedRecords("file.parquet", LongStream.range(0, 50).map(n -> n * 2));
            sleeper.ingest().toStateStore().addFileOnPartition("file.parquet", "root", 50);
            // And a file in each leaf partition
            sleeper.updateTableProperties(Map.of(INGEST_FILE_WRITING_STRATEGY, ONE_FILE_PER_LEAF.toString()));
            sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 50).map(n -> n * 2 + 1));

            // When we split the file from the root partition into separate references in the leaf partitions
            // And we run compaction
            sleeper.compaction()
                    .createJobs(0).createJobs(4) // Split down two levels of the tree
                    .waitForTasks(1).waitForJobs();

            // Then the same records should be present, in one file on each leaf partition
            assertThat(sleeper.directQuery().allRecordsInTable())
                    .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 100)));
            Approvals.verify(printFiles(sleeper.partitioning().tree(), sleeper.tableFiles().all()));
        }
    }
}
