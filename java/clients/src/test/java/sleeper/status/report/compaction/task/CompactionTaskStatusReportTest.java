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
package sleeper.status.report.compaction.task;

import org.junit.Test;
import sleeper.ToStringPrintStream;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.status.report.CompactionTaskStatusReport;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.ClientTestUtils.example;
import static sleeper.status.report.compaction.task.CompactionTaskStatusReportTestHelper.finishedSplittingTask;
import static sleeper.status.report.compaction.task.CompactionTaskStatusReportTestHelper.finishedSplittingTaskWithFourRuns;
import static sleeper.status.report.compaction.task.CompactionTaskStatusReportTestHelper.finishedTask;
import static sleeper.status.report.compaction.task.CompactionTaskStatusReportTestHelper.finishedTaskWithFourRuns;
import static sleeper.status.report.compaction.task.CompactionTaskStatusReportTestHelper.startedSplittingTask;
import static sleeper.status.report.compaction.task.CompactionTaskStatusReportTestHelper.startedTask;

public class CompactionTaskStatusReportTest {

    private final CompactionTaskStatusStore store = mock(CompactionTaskStatusStore.class);

    @Test
    public void shouldReportCompactionTaskUnfinished() throws Exception {
        // Given
        CompactionTaskStatus task = startedTask("A", "2022-10-06T12:17:00.001Z");
        when(store.getTasksInProgress()).thenReturn(Collections.singletonList(task));

        // When / Then
        assertThat(getStandardReport(CompactionTaskQuery.UNFINISHED)).hasToString(
                example("reports/compaction/task/singleTaskUnfinished.txt"));
        assertThat(getJsonReport(CompactionTaskQuery.UNFINISHED)).hasToString(
                example("reports/compaction/task/singleTaskUnfinished.json"));
    }

    @Test
    public void shouldReportCompactionTaskUnfinishedAndFinished() throws Exception {
        // Given
        CompactionTaskStatus unfinishedTask = startedTask("unfinished-task", "2022-10-06T12:17:00.001Z");
        CompactionTaskStatus finishedTask = finishedTask("finished-task", "2022-10-06T12:20:00.001Z",
                "2022-10-06T12:20:30.001Z", 200L, 100L);
        when(store.getAllTasks()).thenReturn(Arrays.asList(finishedTask, unfinishedTask));

        // When / Then
        assertThat(getStandardReport(CompactionTaskQuery.ALL)).hasToString(
                example("reports/compaction/task/unfinishedAndFinished.txt"));
        assertThat(getJsonReport(CompactionTaskQuery.ALL)).hasToString(
                example("reports/compaction/task/unfinishedAndFinished.json"));
    }

    @Test
    public void shouldReportMixedTypesOfCompactionTask() throws Exception {
        // Given
        CompactionTaskStatus unfinishedTask = startedTask("A", "2022-10-06T12:18:00.001Z");
        CompactionTaskStatus finishedTask = finishedTask("B", "2022-10-06T12:20:00.001Z",
                "2022-10-06T12:20:30.001Z", 200L, 100L);
        CompactionTaskStatus unfinishedSplittingTask = startedSplittingTask("C", "2022-10-06T12:22:00.001Z");
        CompactionTaskStatus finishedSplittingTask = finishedSplittingTask("D", "2022-10-06T12:24:00.001Z",
                "2022-10-06T12:24:30.001Z", 400L, 200L);
        when(store.getAllTasks()).thenReturn(Arrays.asList(finishedSplittingTask, unfinishedSplittingTask, finishedTask, unfinishedTask));

        // When / Then
        assertThat(getStandardReport(CompactionTaskQuery.ALL)).hasToString(
                example("reports/compaction/task/mixedTypes.txt"));
        assertThat(getJsonReport(CompactionTaskQuery.ALL)).hasToString(
                example("reports/compaction/task/mixedTypes.json"));
    }

    @Test
    public void shouldReportMultipleJobRunsOnCompactionTasks() throws Exception {
        // Given
        CompactionTaskStatus finishedTask = finishedTaskWithFourRuns("A", "2022-10-06T12:20:00.001Z",
                "2022-10-06T12:20:40.001Z", 800L, 400L);
        CompactionTaskStatus finishedSplittingTask = finishedSplittingTaskWithFourRuns("B", "2022-10-06T12:24:00.001Z",
                "2022-10-06T12:24:40.001Z", 1600L, 800L);
        when(store.getAllTasks()).thenReturn(Arrays.asList(finishedSplittingTask, finishedTask));

        // When / Then
        assertThat(getStandardReport(CompactionTaskQuery.ALL)).hasToString(
                example("reports/compaction/task/multipleJobRunsOnTasks.txt"));
        assertThat(getJsonReport(CompactionTaskQuery.ALL)).hasToString(
                example("reports/compaction/task/multipleJobRunsOnTasks.json"));
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
