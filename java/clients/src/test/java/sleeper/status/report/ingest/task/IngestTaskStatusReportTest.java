/*
 * Copyright 2022 Crown Copyright
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
package sleeper.status.report.ingest.task;

import org.junit.Test;
import sleeper.ToStringPrintStream;
import sleeper.status.report.IngestTaskStatusReport;

import java.io.PrintStream;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ClientTestUtils.example;

public class IngestTaskStatusReportTest {

    @Test
    public void shouldReportNoIngestTasks() throws Exception {

        // When / Then
        assertThat(getStandardReport(IngestTaskQuery.ALL)).hasToString(
                example("reports/ingest/task/noTasksQueryingForAll.txt"));
    }

    private String getStandardReport(IngestTaskQuery query) {
        return getReport(query, StandardIngestTaskStatusReporter::new);
    }

    private String getReport(IngestTaskQuery query, Function<PrintStream, StandardIngestTaskStatusReporter> getReporter) {
        ToStringPrintStream output = new ToStringPrintStream();
        new IngestTaskStatusReport(
                getReporter.apply(output.getPrintStream()),
                query).run();
        return output.toString();
    }
}
