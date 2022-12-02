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
import sleeper.ingest.task.IngestTaskStatus;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.status.report.IngestTaskStatusReport;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.ClientTestUtils.example;
import static sleeper.status.report.ingest.task.IngestTaskStatusReportTestHelper.finishedTask;
import static sleeper.status.report.ingest.task.IngestTaskStatusReportTestHelper.finishedTaskWithFourRuns;
import static sleeper.status.report.ingest.task.IngestTaskStatusReportTestHelper.startedTask;

public class IngestTaskStatusReportTest {
    private final IngestTaskStatusStore store = mock(IngestTaskStatusStore.class);

    @Test
    public void shouldReportNoIngestTasks() throws Exception {

        // When / Then
        assertThat(getStandardReport(IngestTaskQuery.ALL)).hasToString(
                example("reports/ingest/task/noTasksQueryingForAll.txt"));
        assertThat(getJsonReport(IngestTaskQuery.ALL)).hasToString(
                example("reports/ingest/task/noTasksQueryingForAll.json"));
    }

    @Test
    public void shouldReportIngestTaskUnfinishedAndFinished() throws Exception {
        // Given
        IngestTaskStatus unfinished = startedTask("unfinished-task", "2022-10-06T12:17:00.001Z");
        IngestTaskStatus finished = finishedTask("finished-task", "2022-10-06T12:20:00.001Z",
                "2022-10-06T12:20:30.001Z", 200L, 100L);
        when(store.getAllTasks()).thenReturn(Arrays.asList(finished, unfinished));

        // When / Then
        assertThat(getStandardReport(IngestTaskQuery.ALL)).hasToString(
                example("reports/ingest/task/unfinishedAndFinished.txt"));
        assertThat(getJsonReport(IngestTaskQuery.ALL)).hasToString(
                example("reports/ingest/task/unfinishedAndFinished.json"));
    }

    @Test
    public void shouldReportMultipleJobRunsOnIngestTasks() throws Exception {
        // Given
        IngestTaskStatus finished1 = finishedTaskWithFourRuns("A", "2022-10-06T12:20:00.001Z",
                "2022-10-06T12:20:40.001Z", 800L, 400L);
        IngestTaskStatus finished2 = finishedTaskWithFourRuns("B", "2022-10-06T12:22:00.001Z",
                "2022-10-06T12:22:40.001Z", 1600L, 800L);
        when(store.getAllTasks()).thenReturn(Arrays.asList(finished2, finished1));

        // When / Then
        assertThat(getStandardReport(IngestTaskQuery.ALL)).hasToString(
                example("reports/ingest/task/multipleJobRunsOnTasks.txt"));
        assertThat(getJsonReport(IngestTaskQuery.ALL)).hasToString(
                example("reports/ingest/task/multipleJobRunsOnTasks.json"));
    }

    private String getStandardReport(IngestTaskQuery query) {
        return getReport(query, StandardIngestTaskStatusReporter::new);
    }

    private String getJsonReport(IngestTaskQuery query) {
        return getReport(query, JsonIngestTaskStatusReporter::new);
    }

    private String getReport(IngestTaskQuery query, Function<PrintStream, IngestTaskStatusReporter> getReporter) {
        ToStringPrintStream output = new ToStringPrintStream();
        new IngestTaskStatusReport(store,
                getReporter.apply(output.getPrintStream()), query
        ).run();
        return output.toString();
    }
}
