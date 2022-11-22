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
package sleeper.status.report.compactiontask;

import org.junit.Test;
import sleeper.ToStringPrintStream;
import sleeper.compaction.task.CompactionTaskFinishedStatus;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.status.report.CompactionTaskStatusReport;

import java.io.PrintStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.ClientTestUtils.example;

public class CompactionTaskStatusReportTest {

    private final CompactionTaskStatusStore store = mock(CompactionTaskStatusStore.class);

    @Test
    public void shouldReportCompactionTaskUnfinished() throws Exception {
        // Given
        CompactionTaskStatus task = CompactionTaskStatus.builder()
                .started(Instant.parse("2022-10-06T12:17:00.001Z"))
                .taskId("A").build();
        when(store.getTasksInProgress()).thenReturn(Collections.singletonList(task));

        // When / Then
        assertThat(getStandardReport(CompactionTaskQuery.UNFINISHED)).hasToString(
                example("reports/compactiontaskstatus/singleTaskUnfinished.txt"));
        assertThat(getJsonReport(CompactionTaskQuery.UNFINISHED)).hasToString(
                example("reports/compactiontaskstatus/singleTaskUnfinished.json"));
    }

    @Test
    public void shouldReportCompactionTaskUnfinishedAndFinished() throws Exception {
        // Given
        CompactionTaskStatus unfinishedTask = CompactionTaskStatus.builder()
                .started(Instant.parse("2022-10-06T12:17:00.001Z"))
                .taskId("unfinished-task").build();
        CompactionTaskStatus finishedTask = CompactionTaskStatus.builder()
                .started(Instant.parse("2022-10-06T12:20:00.001Z"))
                .taskId("finished-task")
                .finished(CompactionTaskFinishedStatus.builder()
                                .addJobSummary(new RecordsProcessedSummary(
                                        new RecordsProcessed(200L, 100L),
                                        Instant.parse("2022-10-06T12:20:00.001Z"),
                                        Instant.parse("2022-10-06T12:20:30.001Z"))),
                        Instant.parse("2022-10-06T12:20:30.001Z")).build();
        when(store.getAllTasks()).thenReturn(Arrays.asList(unfinishedTask, finishedTask));

        // When / Then
        assertThat(getStandardReport(CompactionTaskQuery.ALL)).hasToString(
                example("reports/compactiontaskstatus/unfinishedAndFinished.txt"));
        assertThat(getJsonReport(CompactionTaskQuery.ALL)).hasToString(
                example("reports/compactiontaskstatus/unfinishedAndFinished.json"));
    }

    private String getStandardReport(CompactionTaskQuery query) {
        return getReport(query, StandardCompactionTaskStatusReporter::new);
    }

    private String getJsonReport(CompactionTaskQuery query) {
        return getReport(query, JsonCompactionTaskStatusReporter::new);
    }

    private String getReport(CompactionTaskQuery query, Function<PrintStream, CompactionTaskStatusReporter> getReporter) {
        ToStringPrintStream output = new ToStringPrintStream();
        new CompactionTaskStatusReport(store,
                getReporter.apply(output.getPrintStream()),
                query).run();
        return output.toString();
    }

}
