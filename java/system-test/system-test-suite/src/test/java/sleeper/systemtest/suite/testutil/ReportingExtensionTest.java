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

package sleeper.systemtest.suite.testutil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import sleeper.systemtest.drivers.instance.ReportingContext;
import sleeper.systemtest.drivers.instance.SystemTestReport;
import sleeper.systemtest.suite.dsl.reports.SystemTestReports;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.suite.testutil.TestContextFactory.testContext;

public class ReportingExtensionTest {

    @TempDir
    private static Path TEMP_DIR;

    @Nested
    @DisplayName("Run reports as a JUnit extension")
    class RunAsExtension {

        @RegisterExtension
        public final ReportingExtension extension = ReportingExtension.reportAlways(
                SystemTestReports.builder(new ReportingContext(TEMP_DIR))
                        .report(fixedReport("test report")));

        @Test
        void shouldOutputAReport() {
            // Reporting handled by extension
        }
    }

    @AfterAll
    static void afterAll() {
        assertThat(TEMP_DIR.resolve("ReportingExtensionTest.RunAsExtension.shouldOutputAReport.report.log"))
                .hasContent("test report");
    }

    @Nested
    @DisplayName("Only output a report when a test failed")
    class OnlyReportWhenTestFailed {

        @TempDir
        private Path tempDir;

        @Test
        void shouldNotOutputReportWhenTestPassed(TestInfo info) {
            extensionReportIfFailed("test passed").afterTestPassed(testContext(info));
            assertThat(tempDir).isEmptyDirectory();
        }

        @Test
        void shouldOutputReportWhenTestFailed(TestInfo info) {
            extensionReportIfFailed("test failed").afterTestFailed(testContext(info));
            assertThat(tempDir.resolve("ReportingExtensionTest.OnlyReportWhenTestFailed.shouldOutputReportWhenTestFailed.report.log"))
                    .hasContent("test failed");
        }

        private ReportingExtension extensionReportIfFailed(String report) {
            return ReportingExtension.reportIfFailed(
                    SystemTestReports.builder(new ReportingContext(tempDir))
                            .report(fixedReport(report)));
        }
    }

    private static SystemTestReport fixedReport(String report) {
        return (out, startTime) -> out.print(report);
    }
}
