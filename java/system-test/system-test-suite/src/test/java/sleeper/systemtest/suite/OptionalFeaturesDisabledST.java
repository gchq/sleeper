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

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.sourcedata.RowNumbers;
import sleeper.systemtest.suite.testutil.SystemTest;
import sleeper.systemtest.suite.testutil.parallel.Slow1;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.sumFileReferenceRowCounts;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.OPTIONAL_FEATURES_DISABLED;

@SystemTest
// Slow because it deploys a separate instance just for this test, and the CDK is slow
@Slow1
public class OptionalFeaturesDisabledST {

    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp(SleeperDsl sleeper, AfterTestReports reporting) {
        sleeper.connectToInstanceAddOnlineTable(OPTIONAL_FEATURES_DISABLED);
    }

    @Test
    void shouldIngest1FileFromDataBucketWhenSourceBucketAndTrackerAreDisabled(SleeperDsl sleeper) throws Exception {
        // Given
        sleeper.sourceFiles().inDataBucket()
                .createWithNumberedRows("file.parquet", LongStream.range(0, 100));

        // When
        sleeper.ingest().byQueue().sendSourceFiles("file.parquet");

        // Then
        sleeper.tableFiles().waitForState(
                files -> files.countFileReferences() > 0,
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(20), Duration.ofMinutes(6)));
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRows().iterableOverRange(0, 100));
        assertThat(sleeper.tableFiles().references()).hasSize(1);
    }

    @Test
    void shouldAllowForCompactionWhenTrackerIsDisabled(SleeperDsl sleeper) throws Exception {
        // Given
        sleeper.updateTableProperties(Map.of(COMPACTION_FILES_BATCH_SIZE, "5"));
        // Files with rows 9, 9, 9, 9, 10 (which match SizeRatioStrategy criteria)
        RowNumbers numbers = sleeper.scrambleNumberedRows(LongStream.range(0, 46));

        // When
        sleeper.ingest().direct(tempDir)
                .numberedRows(numbers.range(0, 9))
                .numberedRows(numbers.range(9, 18))
                .numberedRows(numbers.range(18, 27))
                .numberedRows(numbers.range(27, 36))
                .numberedRows(numbers.range(36, 46));

        // Then
        sleeper.tableFiles().waitForState(
                files -> sumFileReferenceRowCounts(files) == 46
                        && files.getFilesWithReferences().size() == 1,
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(20), Duration.ofMinutes(10)));
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRows().iterableOverRange(0, 46));
    }
}
