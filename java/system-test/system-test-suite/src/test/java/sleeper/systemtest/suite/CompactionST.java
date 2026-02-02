/*
 * Copyright 2022-2025 Crown Copyright
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

package sleeper.systemtest.suite;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.compaction.core.job.creation.strategy.impl.BasicCompactionStrategy;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.row.testutils.SortedRowsCheck;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.nio.file.Path;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.model.IngestFileWritingStrategy.ONE_FILE_PER_LEAF;
import static sleeper.core.properties.model.IngestFileWritingStrategy.ONE_REFERENCE_PER_LEAF;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.core.properties.table.TableProperty.DATA_ENGINE;
import static sleeper.core.properties.table.TableProperty.INGEST_FILE_WRITING_STRATEGY;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.sumFileReferenceRowCounts;
import static sleeper.core.testutils.printers.FileReferencePrinter.printFiles;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;
import static sleeper.systemtest.dsl.util.SystemTestSchema.ROW_KEY_FIELD_NAME;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.testutil.TestResources.exampleString;

@SystemTest
public class CompactionST {
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp(SleeperDsl sleeper, AfterTestReports reporting) {
        sleeper.connectToInstanceNoTables(MAIN);
        sleeper.tables().createWithProperties("compaction", DEFAULT_SCHEMA, Map.of(
                TABLE_ONLINE, "false",
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "2"));
        reporting.reportIfTestFailed(SystemTestReports.SystemTestBuilder::compactionTasksAndJobs);
    }

    @Test
    void shouldCompactFilesFromMultiplePartitionsInJava(SleeperDsl sleeper) throws Exception {
        // Given we compact in Java
        sleeper.updateTableProperties(Map.of(
                DATA_ENGINE, DataEngine.JAVA.toString()));
        // And we have 4 leaf partitions, LL, LR, RL, RR
        sleeper.setGeneratorOverrides(overrideField(
                ROW_KEY_FIELD_NAME, numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
        sleeper.partitioning().setPartitions(new PartitionsBuilder(DEFAULT_SCHEMA)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "row-50")
                .splitToNewChildren("L", "LL", "LR", "row-25")
                .splitToNewChildren("R", "RL", "RR", "row-75")
                .buildTree());
        // And half the partitions have a file A wholly on each partition
        sleeper.updateTableProperties(Map.of(INGEST_FILE_WRITING_STRATEGY, ONE_FILE_PER_LEAF.toString()));
        sleeper.ingest().direct(tempDir)
                .numberedRows(LongStream.range(0, 50));
        // And we have a file B containing data for all partitions, referenced on each
        sleeper.updateTableProperties(Map.of(INGEST_FILE_WRITING_STRATEGY, ONE_REFERENCE_PER_LEAF.toString()));
        sleeper.ingest().direct(tempDir)
                .numberedRows(sleeper.scrambleNumberedRows(LongStream.range(0, 100)).stream());
        // And we have a file C in the root partition
        sleeper.sourceFiles().inDataBucket().writeSketches()
                .createWithNumberedRows("file.parquet", LongStream.range(50, 100));
        sleeper.ingest().toStateStore().addFileOnPartition("file.parquet", "root", 50);

        // When
        sleeper.compaction()
                // Merge files A and B on LL and LR, split C one level down to L and R
                .createJobs(2).waitForTasks(1).waitForJobs()
                // Split C another level to LL and LR, merge it with the existing data
                .createJobs(4).waitForTasks(1).waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRows().iterableFrom(
                        () -> LongStream.range(0, 100)
                                .flatMap(number -> LongStream.of(number, number))));
        AllReferencesToAllFiles files = sleeper.tableFiles().all();
        assertThat(printFiles(sleeper.partitioning().tree(), files))
                .isEqualTo(exampleString("compaction/compactedFilesFromMultiplePartitions.txt"));
        assertThat(files.getFilesWithReferences())
                .allSatisfy(file -> assertThat(
                        SortedRowsCheck.check(DEFAULT_SCHEMA, sleeper.getRows(file)))
                        .isEqualTo(SortedRowsCheck.sorted(sumFileReferenceRowCounts(file))));
    }

    @Test
    void shouldCompactFilesFromMultiplePartitionsInDataFusion(SleeperDsl sleeper) throws Exception {
        // Given we compact in DataFusion
        sleeper.updateTableProperties(Map.of(
                DATA_ENGINE, DataEngine.DATAFUSION.toString()));
        // And we have 4 leaf partitions, LL, LR, RL, RR
        sleeper.setGeneratorOverrides(overrideField(
                ROW_KEY_FIELD_NAME, numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
        sleeper.partitioning().setPartitions(new PartitionsBuilder(DEFAULT_SCHEMA)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "row-50")
                .splitToNewChildren("L", "LL", "LR", "row-25")
                .splitToNewChildren("R", "RL", "RR", "row-75")
                .buildTree());
        // And half the partitions have a file A wholly on each partition
        sleeper.updateTableProperties(Map.of(INGEST_FILE_WRITING_STRATEGY, ONE_FILE_PER_LEAF.toString()));
        sleeper.ingest().direct(tempDir)
                .numberedRows(LongStream.range(0, 50));
        // And we have a file B containing data for all partitions, referenced on each
        sleeper.updateTableProperties(Map.of(INGEST_FILE_WRITING_STRATEGY, ONE_REFERENCE_PER_LEAF.toString()));
        sleeper.ingest().direct(tempDir)
                .numberedRows(sleeper.scrambleNumberedRows(LongStream.range(0, 100)).stream());
        // And we have a file C in the root partition
        sleeper.sourceFiles().inDataBucket().writeSketches()
                .createWithNumberedRows("file.parquet", LongStream.range(50, 100));
        sleeper.ingest().toStateStore().addFileOnPartition("file.parquet", "root", 50);

        // When
        sleeper.compaction()
                // Merge files A and B on LL and LR, split C one level down to L and R
                .createJobs(2).waitForTasks(1).waitForJobs()
                // Split C another level to LL and LR, merge it with the existing data
                .createJobs(4).waitForTasks(1).waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRows().iterableFrom(
                        () -> LongStream.range(0, 100)
                                .flatMap(number -> LongStream.of(number, number))));
        AllReferencesToAllFiles files = sleeper.tableFiles().all();
        assertThat(printFiles(sleeper.partitioning().tree(), files))
                .isEqualTo(exampleString("compaction/compactedFilesFromMultiplePartitions.txt"));
        assertThat(files.getFilesWithReferences())
                .allSatisfy(file -> assertThat(
                        SortedRowsCheck.check(DEFAULT_SCHEMA, sleeper.getRows(file)))
                        .isEqualTo(SortedRowsCheck.sorted(sumFileReferenceRowCounts(file))));
    }
}
