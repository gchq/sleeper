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

import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@SystemTest
public class PythonBulkImportST {

    @BeforeEach
    void setup(SleeperDsl sleeper, AfterTestReports reporting) {
        reporting.reportIfTestFailed(SystemTestReports.SystemTestBuilder::ingestTasksAndJobs);
        sleeper.connectToInstanceAddOnlineTable(MAIN);
    }

    @Test
    void shouldBulkImportFilesFromS3(SleeperDsl sleeper) {
        // Given
        sleeper.updateTableProperties(Map.of(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "1"));
        sleeper.sourceFiles()
                .createWithNumberedRows("file1.parquet", LongStream.range(0, 100))
                .createWithNumberedRows("file2.parquet", LongStream.range(100, 200));

        // When
        sleeper.pythonApi()
                .bulkImport().fromS3("file1.parquet", "file2.parquet")
                .waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRows().iterableOverRange(0, 200));
        assertThat(sleeper.tableFiles().references()).hasSize(1);
    }
}
