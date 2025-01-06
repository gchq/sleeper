/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.clients.status.report.ingest.task;

import org.junit.jupiter.api.Test;

import sleeper.clients.status.report.IngestTaskStatusReport;
import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.ingest.core.task.IngestTaskStatus;
import sleeper.ingest.core.task.IngestTaskTracker;

import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.clients.testutil.ClientTestUtils.example;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;

public class IngestTaskStatusReportTest {
    private final IngestTaskTracker store = mock(IngestTaskTracker.class);

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
        IngestTaskStatus unfinished = IngestTaskStatusReportTestHelper.startedTask("unfinished-task", "2022-10-06T12:17:00.001Z");
        IngestTaskStatus finished = IngestTaskStatusReportTestHelper.finishedTask("finished-task", "2022-10-06T12:20:00.001Z",
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
        IngestTaskStatus finished1 = IngestTaskStatusReportTestHelper.finishedTask("A",
                "2022-10-06T12:20:00.001Z", "2022-10-06T12:20:50.001Z",
                summary(Instant.parse("2022-10-06T12:20:01.001Z"), Duration.ofSeconds(10), 200, 100),
                summary(Instant.parse("2022-10-06T12:20:12.001Z"), Duration.ofSeconds(10), 200, 100),
                summary(Instant.parse("2022-10-06T12:20:23.001Z"), Duration.ofSeconds(10), 200, 100),
                summary(Instant.parse("2022-10-06T12:20:34.001Z"), Duration.ofSeconds(10), 200, 100));
        IngestTaskStatus finished2 = IngestTaskStatusReportTestHelper.finishedTask("B",
                "2022-10-06T12:22:00.001Z", "2022-10-06T12:22:50.001Z",
                summary(Instant.parse("2022-10-06T12:22:01.001Z"), Duration.ofSeconds(10), 400, 200),
                summary(Instant.parse("2022-10-06T12:22:12.001Z"), Duration.ofSeconds(10), 400, 200),
                summary(Instant.parse("2022-10-06T12:22:23.001Z"), Duration.ofSeconds(10), 400, 200),
                summary(Instant.parse("2022-10-06T12:22:34.001Z"), Duration.ofSeconds(10), 400, 200));
        when(store.getAllTasks()).thenReturn(Arrays.asList(finished2, finished1));

        // When / Then
        assertThat(getStandardReport(IngestTaskQuery.ALL)).hasToString(
                example("reports/ingest/task/multipleJobRunsOnTasks.txt"));
        assertThat(getJsonReport(IngestTaskQuery.ALL)).hasToString(
                example("reports/ingest/task/multipleJobRunsOnTasks.json"));
    }

    @Test
    public void shouldReportUnfinishedIngestTasks() throws Exception {
        // Given
        IngestTaskStatus unfinished1 = IngestTaskStatusReportTestHelper.startedTask("A", "2022-10-06T12:17:00.001Z");
        IngestTaskStatus unfinished2 = IngestTaskStatusReportTestHelper.startedTask("B", "2022-10-06T12:20:00.001Z");

        when(store.getTasksInProgress()).thenReturn(Arrays.asList(unfinished2, unfinished1));

        // When / Then
        assertThat(getStandardReport(IngestTaskQuery.UNFINISHED)).hasToString(
                example("reports/ingest/task/unfinishedTasks.txt"));
        assertThat(getJsonReport(IngestTaskQuery.UNFINISHED)).hasToString(
                example("reports/ingest/task/unfinishedTasks.json"));
    }

    private String getStandardReport(IngestTaskQuery query) {
        return getReport(query, StandardIngestTaskStatusReporter::new);
    }

    private String getJsonReport(IngestTaskQuery query) {
        return getReport(query, JsonIngestTaskStatusReporter::new);
    }

    private String getReport(IngestTaskQuery query, Function<PrintStream, IngestTaskStatusReporter> getReporter) {
        ToStringConsoleOutput output = new ToStringConsoleOutput();
        new IngestTaskStatusReport(store,
                getReporter.apply(output.getPrintStream()), query).run();
        return output.toString();
    }
}
