/*
 * Copyright 2022-2026 Crown Copyright
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

package sleeper.systemtest.drivers.nightly;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.systemtest.drivers.nightly.NightlyTestOutputTestHelper.testResultWithShortId;

class NightlyTestSummaryTableTest {
    @Nested
    @DisplayName("Output summary as json")
    class OutputJson {
        @Test
        void shouldCreateSummaryWithSingleNightlyExecution() {
            // Given
            NightlyTestSummaryTable summary = NightlyTestSummaryTable.empty().add(
                    NightlyTestTimestamp.from(Instant.parse("2023-05-03T15:15:00Z")),
                    NightlyTestOutputTestHelper.outputWithStatusCodeByTest(Map.of("bulkImportPerformance", 0)));

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
                            "}]}")
                    .add(
                            NightlyTestTimestamp.from(Instant.parse("2023-05-04T15:42:00Z")),
                            NightlyTestOutputTestHelper.outputWithStatusCodeByTest(Map.of("bulkImportPerformance", 1)));

            // When / Then
            assertThatJson(summary.toJson())
                    .isEqualTo("{\"executions\":[{" +
                            "\"startTime\":\"2023-05-04T15:42:00Z\"," +
                            "\"tests\": [{\"name\":\"bulkImportPerformance\", \"exitCode\":1}]" +
                            "},{" +
                            "\"startTime\":\"2023-05-03T15:15:00Z\"," +
                            "\"tests\": [{\"name\":\"bulkImportPerformance\", \"exitCode\":0}]" +
                            "}]}");
        }

        @Test
        void shouldRecordShortId() {
            // Given
            NightlyTestSummaryTable summary = NightlyTestSummaryTable.empty().add(
                    NightlyTestTimestamp.from(Instant.parse("2023-05-22T16:14:00Z")),
                    new NightlyTestOutput(List.of(TestResult.builder()
                            .testName("bulkImportPerformance")
                            .shortId("bulk-import-instance")
                            .exitCode(0)
                            .build())));

            // When / Then
            assertThatJson(summary.toJson())
                    .isEqualTo("{\"executions\":[{" +
                            "\"startTime\":\"2023-05-22T16:14:00Z\"," +
                            "\"tests\": [{" +
                            "\"name\":\"bulkImportPerformance\", " +
                            "\"exitCode\":0, " +
                            "\"shortId\":\"bulk-import-instance\"" +
                            "}]" +
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
                            NightlyTestOutputTestHelper.outputWithStatusCodeByTest(Map.of("bulkImportPerformance", 0)))
                    .add(
                            NightlyTestTimestamp.from(Instant.parse("2023-05-04T15:42:00Z")),
                            NightlyTestOutputTestHelper.outputWithStatusCodeByTest(Map.of("bulkImportPerformance", 1)));

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
                            NightlyTestOutputTestHelper.outputWithStatusCodeByTest(Map.of(
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

    @Nested
    @DisplayName("Check recent test suite runs")
    class CheckTests {

        @Test
        void shouldPassWhenOneRunSucceededInLastDay() {
            // Given
            NightlyTestSummaryTable summary = NightlyTestSummaryTable.empty()
                    .add(
                            NightlyTestTimestamp.from(Instant.parse("2026-06-17T20:03:00Z")),
                            NightlyTestOutputTestHelper.outputWithStatusCodeByTest(Map.of(
                                    "splittingPerformance", 0,
                                    "bulkImportPerformance", 0,
                                    "compactionPerformance", 0)));
            Instant now = Instant.parse("2026-06-18T12:13:00Z");

            // When / Then
            assertThatCode(() -> summary.checkPassedRecently(now))
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldFailWhenOneRunFailedInLastDay() {
            // Given
            NightlyTestSummaryTable summary = NightlyTestSummaryTable.empty()
                    .add(
                            NightlyTestTimestamp.from(Instant.parse("2026-06-17T20:03:00Z")),
                            new NightlyTestOutput(List.of(
                                    testResultWithShortId("splittingPerformance", "aa06172003sp", 1),
                                    testResultWithShortId("bulkImportPerformance", "aa06172003bp", 0),
                                    testResultWithShortId("compactionPerformance", "aa06172003cp", 0))));
            Instant now = Instant.parse("2026-06-18T12:13:00Z");

            // When / Then
            assertThatThrownBy(() -> summary.checkPassedRecently(now))
                    .isInstanceOf(LastRunFailedException.class)
                    .hasMessage("Last run failed, started at 2026-06-17T20:03:00Z, failed test suites: splittingPerformance (short ID aa06172003sp)");
        }

        @Test
        void shouldFailWithNoRuns() {
            // Given
            NightlyTestSummaryTable summary = NightlyTestSummaryTable.empty();
            Instant now = Instant.parse("2026-06-18T12:13:00Z");

            // When / Then
            assertThatThrownBy(() -> summary.checkPassedRecently(now))
                    .isInstanceOf(NoRecentRunException.class)
                    .hasMessage("No test runs found");
        }

        @Test
        void shouldFailWithRunOlderThanOneDay() {
            // Given
            NightlyTestSummaryTable summary = NightlyTestSummaryTable.empty()
                    .add(
                            NightlyTestTimestamp.from(Instant.parse("2026-06-16T20:03:00Z")),
                            NightlyTestOutputTestHelper.outputWithStatusCodeByTest(Map.of(
                                    "splittingPerformance", 0,
                                    "bulkImportPerformance", 0,
                                    "compactionPerformance", 0)));
            Instant now = Instant.parse("2026-06-18T12:13:00Z");

            // When / Then
            assertThatThrownBy(() -> summary.checkPassedRecently(now))
                    .isInstanceOf(NoRecentRunException.class)
                    .hasMessage("No recent test run found, last run time: 2026-06-16T20:03:00Z");
        }
    }
}
