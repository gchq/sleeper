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

package sleeper.systemtest.suite;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.dsl.sourcedata.RecordNumbers;
import sleeper.systemtest.suite.testutil.Slow;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.nio.file.Path;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.testutils.printers.FileReferencePrinter.printFiles;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.OPTIONAL_FEATURES_DISABLED;
import static sleeper.systemtest.suite.testutil.TestResources.exampleString;

@SystemTest
@Slow // Slow because it deploys a separate instance just for this test, and the CDK is slow
public class OptionalFeaturesDisabledST {

    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting) {
        sleeper.connectToInstance(OPTIONAL_FEATURES_DISABLED);
        reporting.reportAlways(SystemTestReports.SystemTestBuilder::ingestTasksAndJobs);
    }

    @Test
    void shouldIngest1FileFromDataBucketWhenSourceBucketAndStatusStoreDisabled(SleeperSystemTest sleeper) {
        // Given
        sleeper.sourceFiles().inDataBucket()
                .createWithNumberedRecords("file.parquet", LongStream.range(0, 100));

        // When
        sleeper.ingest().byQueue().sendSourceFiles("file.parquet")
                .invokeTask().waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 100)));
        assertThat(sleeper.tableFiles().references()).hasSize(1);
    }

    @Test
    void shouldAllowForCompactionWhenStatusStoreDisabled(SleeperSystemTest sleeper) {
        // Given
        sleeper.updateTableProperties(Map.of(COMPACTION_FILES_BATCH_SIZE, "5"));
        // Files with records 9, 9, 9, 9, 10 (which match SizeRatioStrategy criteria)
        RecordNumbers numbers = sleeper.scrambleNumberedRecords(LongStream.range(0, 46));
        sleeper.ingest().direct(tempDir)
                .numberedRecords(numbers.range(0, 9))
                .numberedRecords(numbers.range(9, 18))
                .numberedRecords(numbers.range(18, 27))
                .numberedRecords(numbers.range(27, 36))
                .numberedRecords(numbers.range(36, 46));

        // When
        sleeper.compaction().createJobs(1).invokeTasks(1).waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 46)));
        assertThat(printFiles(sleeper.partitioning().tree(), sleeper.tableFiles().all()))
                .isEqualTo(exampleString("compaction/compacted5ToSingleFile.txt"));
    }
}
