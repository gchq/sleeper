/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.systemtest.output;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.output.NightlyTestOutputTestHelper.outputWithStatusCodeByTest;

class NightlyTestSummaryTableTest {
    @Nested
    @DisplayName("Output summary as json")
    class OutputJson {
        @Test
        void shouldCreateSummaryWithSingleNightlyExecution() {
            // Given
            NightlyTestSummaryTable summary = NightlyTestSummaryTable.empty().add(
                    NightlyTestTimestamp.from(Instant.parse("2023-05-03T15:15:00Z")),
                    outputWithStatusCodeByTest(Map.of("bulkImportPerformance", 0)));

            // When / Then
            assertThatJson(summary.toJson())
                    .isEqualTo("{\"executions\":[{" +
                            "\"startTime\":\"2023-05-03T15:15:00Z\"," +
                            "\"tests\": [{\"name\":\"bulkImportPerformance\", \"exitCode\":0}]" +
                            "}]}");
        }

        @Test
        void shouldAddASecondNightlyExecution() {
            NightlyTestSummaryTable summary = NightlyTestSummaryTable.fromJson(
                    "{\"executions\":[{" +
                            "\"startTime\":\"2023-05-03T15:15:00Z\"," +
                            "\"tests\": [{\"name\":\"bulkImportPerformance\", \"exitCode\":0}]" +
                            "}]}").add(
                    NightlyTestTimestamp.from(Instant.parse("2023-05-04T15:42:00Z")),
                    outputWithStatusCodeByTest(Map.of("bulkImportPerformance", 1)));

            // When / Then
            assertThatJson(summary.toJson())
                    .isEqualTo("{\"executions\":[{" +
                            "\"startTime\":\"2023-05-03T15:15:00Z\"," +
                            "\"tests\": [{\"name\":\"bulkImportPerformance\", \"exitCode\":0}]" +
                            "},{" +
                            "\"startTime\":\"2023-05-04T15:42:00Z\"," +
                            "\"tests\": [{\"name\":\"bulkImportPerformance\", \"exitCode\":1}]" +
                            "}]}");
        }
    }

    @Nested
    @DisplayName("Output summary as table")
    class OutputTable {
        @Test
        void shouldFormatAsATableWithMultipleExecutions() {
            // Given
            NightlyTestSummaryTable summary = NightlyTestSummaryTable.empty()
                    .add(
                            NightlyTestTimestamp.from(Instant.parse("2023-05-03T15:15:00Z")),
                            outputWithStatusCodeByTest(Map.of("bulkImportPerformance", 0)))
                    .add(
                            NightlyTestTimestamp.from(Instant.parse("2023-05-04T15:42:00Z")),
                            outputWithStatusCodeByTest(Map.of("bulkImportPerformance", 1)));

            // When / Then
            assertThat(summary.toTableString()).isEqualTo("" +
                    "------------------------------------------------\n" +
                    "| START_TIME           | bulkImportPerformance |\n" +
                    "| 2023-05-04T15:42:00Z | FAILED                |\n" +
                    "| 2023-05-03T15:15:00Z | PASSED                |\n" +
                    "------------------------------------------------\n");
        }

        @Test
        void shouldFormatAsATableWithMultipleTestsInOneExecutionOrderedByName() {
            // Given
            NightlyTestSummaryTable summary = NightlyTestSummaryTable.empty()
                    .add(
                            NightlyTestTimestamp.from(Instant.parse("2023-05-03T15:15:00Z")),
                            outputWithStatusCodeByTest(Map.of(
                                    "splittingPerformance", 0,
                                    "bulkImportPerformance", 0,
                                    "compactionPerformance", 0)));

            // When / Then
            assertThat(summary.toTableString()).isEqualTo("" +
                    "-----------------------------------------------------------------------------------------------\n" +
                    "| START_TIME           | bulkImportPerformance | compactionPerformance | splittingPerformance |\n" +
                    "| 2023-05-03T15:15:00Z | PASSED                | PASSED                | PASSED               |\n" +
                    "-----------------------------------------------------------------------------------------------\n");
        }
    }
}
