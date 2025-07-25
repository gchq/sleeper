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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.dsl.sourcedata.RowNumbers;
import sleeper.systemtest.suite.testutil.Slow;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.nio.file.Path;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.core.testutils.printers.FileReferencePrinter.printFiles;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.COMPACTION_ON_EC2;
import static sleeper.systemtest.suite.testutil.TestResources.exampleString;

@SystemTest
@Slow
public class CompactionOnEC2ST {
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting) {
        sleeper.connectToInstanceNoTables(COMPACTION_ON_EC2);
        reporting.reportIfTestFailed(SystemTestReports.SystemTestBuilder::compactionTasksAndJobs);
    }

    @AfterEach
    void tearDown(SleeperSystemTest sleeper) {
        sleeper.compaction().scaleToZero();
    }

    @Test
    void shouldCompactFilesUsingDefaultCompactionStrategy(SleeperSystemTest sleeper) {
        // Given
        sleeper.tables().createWithProperties("test", DEFAULT_SCHEMA, Map.of(
                TABLE_ONLINE, "false",
                COMPACTION_FILES_BATCH_SIZE, "5"));
        // Files with rows 9, 9, 9, 9, 10 (which match SizeRatioStrategy criteria)
        RowNumbers numbers = sleeper.scrambleNumberedRows(LongStream.range(0, 46));
        sleeper.ingest().direct(tempDir)
                .numberedRows(numbers.range(0, 9))
                .numberedRows(numbers.range(9, 18))
                .numberedRows(numbers.range(18, 27))
                .numberedRows(numbers.range(27, 36))
                .numberedRows(numbers.range(36, 46));

        // When
        sleeper.compaction().createJobs(1).waitForTasks(1).waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRows(LongStream.range(0, 46)));
        assertThat(printFiles(sleeper.partitioning().tree(), sleeper.tableFiles().all()))
                .isEqualTo(exampleString("compaction/compacted5ToSingleFile.txt"));
    }
}
