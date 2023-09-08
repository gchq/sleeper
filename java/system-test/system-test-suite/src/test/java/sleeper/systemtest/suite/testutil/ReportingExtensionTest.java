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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import sleeper.systemtest.drivers.instance.ReportingContext;
import sleeper.systemtest.drivers.instance.SystemTestReport;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ReportingExtensionTest {

    @TempDir
    private static Path TEMP_DIR;
    private final ReportingContext context = new ReportingContext(TEMP_DIR);

    @RegisterExtension
    private final ReportingExtension extension = new ReportingExtension(context, List.of(fixedReport("test report")));

    @Test
    void shouldOutputAReport() {
        // Reporting handled by extension
    }

    @AfterAll
    static void afterAll() {
        assertThat(TEMP_DIR.resolve("ReportingExtensionTest.shouldOutputAReport.report.log"))
                .content().isEqualTo("test report");
    }

    private static SystemTestReport fixedReport(String report) {
        return (out, startTime) -> out.print(report);
    }
}
