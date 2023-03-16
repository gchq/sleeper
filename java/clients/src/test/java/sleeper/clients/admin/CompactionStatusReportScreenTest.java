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

package sleeper.clients.admin;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import sleeper.clients.admin.testutils.AdminClientMockStoreBase;
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.status.store.job.DynamoDBCompactionJobStatusStore;
import sleeper.compaction.status.store.task.DynamoDBCompactionTaskStatusStore;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.COMPACTION_JOB_STATUS_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.COMPACTION_STATUS_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.COMPACTION_TASK_STATUS_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.EXIT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.JOB_QUERY_ALL_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.JOB_QUERY_DETAILED_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.JOB_QUERY_RANGE_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.JOB_QUERY_UNKNOWN_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_RETURN_TO_MAIN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TASK_QUERY_ALL_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TASK_QUERY_UNFINISHED_OPTION;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.compaction.job.CompactionJobStatusTestData.startedCompactionRun;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.console.ConsoleOutput.CLEAR_CONSOLE;
import static sleeper.status.report.compaction.task.CompactionTaskStatusReportTestHelper.startedTask;

class CompactionStatusReportScreenTest extends AdminClientMockStoreBase {
    @Nested
    @DisplayName("Compaction job status report")
    class CompactionJobStatusReport {
        private final CompactionJobTestDataHelper dataHelper = new CompactionJobTestDataHelper();
        protected final DynamoDBCompactionJobStatusStore compactionJobStatusStore = mock(DynamoDBCompactionJobStatusStore.class);

        @Test
        void shouldRunCompactionJobStatusReportWithQueryTypeAll() {
            // Given
            createCompactionJobStatusStore();
            setTablePropertiesForTable("test-table");
            when(compactionJobStatusStore.getAllJobs("test-table"))
                    .thenReturn(exampleJobStatuses(dataHelper));
            in.enterNextPrompts(COMPACTION_STATUS_REPORT_OPTION,
                    COMPACTION_JOB_STATUS_REPORT_OPTION, "test-table", JOB_QUERY_ALL_OPTION,
                    EXIT_OPTION);

            // When/Then
            String output = runClientGetOutput();
            assertThat(output)
                    .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                    .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                    .contains("Compaction Job Status Report")
                    .contains("" +
                            "Total standard jobs: 1\n" +
                            "Total standard jobs pending: 0\n" +
                            "Total standard jobs in progress: 1\n" +
                            "Total standard jobs finished: 0");

            verifyWithNumberOfInvocations(4);
        }

        @Test
        void shouldRunCompactionJobStatusReportWithQueryTypeUnknown() {
            // Given
            createCompactionJobStatusStore();
            setTablePropertiesForTable("test-table");
            when(compactionJobStatusStore.getUnfinishedJobs("test-table"))
                    .thenReturn(exampleJobStatuses(dataHelper));
            in.enterNextPrompts(COMPACTION_STATUS_REPORT_OPTION,
                    COMPACTION_JOB_STATUS_REPORT_OPTION, "test-table", JOB_QUERY_UNKNOWN_OPTION,
                    EXIT_OPTION);

            // When/Then
            String output = runClientGetOutput();
            assertThat(output)
                    .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                    .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                    .contains("Compaction Job Status Report")
                    .contains("" +
                            "Total unfinished jobs: 1\n" +
                            "Total unfinished jobs in progress: 1\n" +
                            "Total unfinished jobs not started: 0");

            verifyWithNumberOfInvocations(4);
        }

        @Test
        void shouldRunCompactionJobStatusReportWithQueryTypeDetailed() {
            // Given
            createCompactionJobStatusStore();
            setTablePropertiesForTable("test-table");
            List<CompactionJobStatus> jobStatuses = exampleJobStatuses(dataHelper);
            CompactionJobStatus exampleJob = jobStatuses.get(0);
            when(compactionJobStatusStore.getJob(exampleJob.getJobId()))
                    .thenReturn(Optional.of(exampleJob));
            in.enterNextPrompts(COMPACTION_STATUS_REPORT_OPTION,
                    COMPACTION_JOB_STATUS_REPORT_OPTION, "test-table",
                    JOB_QUERY_DETAILED_OPTION, exampleJob.getJobId(),
                    EXIT_OPTION);

            // When/Then
            String output = runClientGetOutput();
            assertThat(output)
                    .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                    .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                    .contains("Compaction Job Status Report")
                    .contains("" +
                            "Details for job " + exampleJob.getJobId());

            verifyWithNumberOfInvocations(5);
        }

        @Test
        void shouldRunCompactionJobStatusReportWithQueryTypeRange() {
            // Given
            createCompactionJobStatusStore();
            setTablePropertiesForTable("test-table");
            when(compactionJobStatusStore.getJobsInTimePeriod("test-table",
                    Instant.parse("2023-03-10T17:52:12Z"), Instant.parse("2023-03-18T17:52:12Z")))
                    .thenReturn(exampleJobStatuses(dataHelper));
            in.enterNextPrompts(COMPACTION_STATUS_REPORT_OPTION,
                    COMPACTION_JOB_STATUS_REPORT_OPTION, "test-table",
                    JOB_QUERY_RANGE_OPTION, "20230310175212", "20230318175212",
                    EXIT_OPTION);

            // When/Then
            String output = runClientGetOutput();
            assertThat(output)
                    .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                    .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                    .contains("Compaction Job Status Report")
                    .contains("" +
                            "Total jobs in defined range: 1");

            verifyWithNumberOfInvocations(6);
        }

        private void createCompactionJobStatusStore() {
            InstanceProperties properties = createValidInstanceProperties();
            when(store.loadCompactionJobStatusStore(properties.get(ID)))
                    .thenReturn(compactionJobStatusStore);
        }

        private void setTablePropertiesForTable(String tableName) {
            InstanceProperties properties = createValidInstanceProperties();
            TableProperties tableProperties = createValidTableProperties(properties, tableName);
            setInstanceProperties(properties);
            when(store.loadTableProperties(properties.get(ID), tableName))
                    .thenReturn(tableProperties);
        }

        private List<CompactionJobStatus> exampleJobStatuses(CompactionJobTestDataHelper dataHelper) {
            return List.of(
                    jobCreated(dataHelper.singleFileCompaction(),
                            Instant.parse("2023-03-15T17:52:12.001Z"),
                            startedCompactionRun("test-task-1", Instant.parse("2023-03-15T17:53:12.001Z"))));
        }
    }

    @Nested
    @DisplayName("Compaction task status report")
    class CompactionTaskStatusReport {
        private final DynamoDBCompactionTaskStatusStore compactionTaskStatusStore = mock(DynamoDBCompactionTaskStatusStore.class);

        @Test
        void shouldRunCompactionTaskStatusReportWithQueryTypeAll() {
            // Given
            createCompactionTaskStatusStore();
            when(compactionTaskStatusStore.getAllTasks())
                    .thenReturn(exampleTaskStatuses());
            in.enterNextPrompts(COMPACTION_STATUS_REPORT_OPTION,
                    COMPACTION_TASK_STATUS_REPORT_OPTION, TASK_QUERY_ALL_OPTION,
                    EXIT_OPTION);

            // When/Then
            String output = runClientGetOutput();
            assertThat(output)
                    .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                    .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                    .contains("Compaction Task Status Report")
                    .contains("" +
                            "Total tasks: 1\n" +
                            "\n" +
                            "Total standard tasks: 1\n" +
                            "Total standard tasks in progress: 1\n" +
                            "Total standard tasks finished: 0\n" +
                            "\n" +
                            "Total splitting tasks: 0\n" +
                            "Total splitting tasks in progress: 0\n" +
                            "Total splitting tasks finished: 0\n");

            verifyWithNumberOfInvocations(3);
        }

        @Test
        void shouldRunCompactionTaskStatusReportWithQueryTypeUnfinished() {
            // Given
            createCompactionTaskStatusStore();
            when(compactionTaskStatusStore.getTasksInProgress())
                    .thenReturn(exampleTaskStatuses());
            in.enterNextPrompts(COMPACTION_STATUS_REPORT_OPTION,
                    COMPACTION_TASK_STATUS_REPORT_OPTION, TASK_QUERY_UNFINISHED_OPTION,
                    EXIT_OPTION);

            // When/Then
            String output = runClientGetOutput();
            assertThat(output)
                    .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                    .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                    .contains("Compaction Task Status Report")
                    .contains("" +
                            "Total tasks in progress: 1\n" +
                            "Total standard tasks in progress: 1\n" +
                            "Total splitting tasks in progress: 0\n");

            verifyWithNumberOfInvocations(3);
        }

        private void createCompactionTaskStatusStore() {
            InstanceProperties properties = createValidInstanceProperties();
            when(store.loadCompactionTaskStatusStore(properties.get(ID)))
                    .thenReturn(compactionTaskStatusStore);
        }

        private List<CompactionTaskStatus> exampleTaskStatuses() {
            return List.of(startedTask("task-1", "2023-03-15T18:53:12.001Z"));
        }
    }

    private void verifyWithNumberOfInvocations(int numberOfInvocations) {
        InOrder order = Mockito.inOrder(in.mock);
        order.verify(in.mock, times(numberOfInvocations)).promptLine(any());
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }
}
