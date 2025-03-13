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

package sleeper.systemtest.dsl.extension;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import sleeper.systemtest.dsl.reporting.ReportingContext;
import sleeper.systemtest.dsl.reporting.SystemTestReport;
import sleeper.systemtest.dsl.reporting.SystemTestReports;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.dsl.extension.TestContextFactory.testContext;

public class AfterTestReportsTest {

    @TempDir
    private Path tempDir;
    private final AfterTestReportsBase<SystemTestReports.Builder> reporting = new AfterTestReportsBase<>(
            () -> SystemTestReports.builder(new ReportingContext(tempDir)));

    @Nested
    @DisplayName("Always output a report")
    class AlwaysReport {

        @Test
        void shouldOutputAReportWhenTestPassed(TestInfo info) {
            reporting.reportAlways(builder -> builder.report(fixedReport("test passed")));
            reporting.afterTestPassed(testContext(info));
            assertThat(tempDir.resolve("AfterTestReportsTest.AlwaysReport.shouldOutputAReportWhenTestPassed.report.log"))
                    .hasContent("test passed");
        }

        @Test
        void shouldOutputAReportWhenTestFailed(TestInfo info) {
            reporting.reportAlways(builder -> builder.report(fixedReport("test failed")));
            reporting.afterTestFailed(testContext(info));
            assertThat(tempDir.resolve("AfterTestReportsTest.AlwaysReport.shouldOutputAReportWhenTestFailed.report.log"))
                    .hasContent("test failed");
        }
    }

    @Nested
    @DisplayName("Only output a report when a test failed")
    class OnlyReportWhenTestFailed {

        @Test
        void shouldNotOutputReportWhenTestPassed(TestInfo info) {
            reporting.reportIfTestFailed(builder -> builder.report(fixedReport("test passed")));
            reporting.afterTestPassed(testContext(info));
            assertThat(tempDir).isEmptyDirectory();
        }

        @Test
        void shouldOutputReportWhenTestFailed(TestInfo info) {
            reporting.reportIfTestFailed(builder -> builder.report(fixedReport("test failed")));
            reporting.afterTestFailed(testContext(info));
            assertThat(tempDir.resolve("AfterTestReportsTest.OnlyReportWhenTestFailed.shouldOutputReportWhenTestFailed.report.log"))
                    .hasContent("test failed");
        }
    }

    private static SystemTestReport fixedReport(String report) {
        return (out, startTime) -> out.print(report);
    }
}
