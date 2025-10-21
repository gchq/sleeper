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

import sleeper.bulkexport.core.model.BulkExportQuery;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@SystemTest
public class BulkExportST {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting) {
        sleeper.connectToInstanceAddOnlineTable(MAIN);
        reporting.reportIfTestFailed(SystemTestReports.SystemTestBuilder::ingestTasksAndJobs);
    }

    @Test
    void shouldExecuteBasicBulkExport(SleeperSystemTest sleeper) {
        // Given
        sleeper.sourceFiles()
                .createWithNumberedRows("file1.parquet", LongStream.range(0, 100))
                .createWithNumberedRows("file2.parquet", LongStream.range(100, 200));

        // When
        sleeper.ingest().byQueue()
                .sendSourceFiles("file1.parquet", "file2.parquet")
                .waitForTask().waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRows(LongStream.range(0, 200)));

        sleeper.bulkExport()
                .bulkExportDriver()
                .sendJob(BulkExportQuery.builder()
                        .tableId("table-1")
                        .exportId("export-1")
                        .build());
    }
}
