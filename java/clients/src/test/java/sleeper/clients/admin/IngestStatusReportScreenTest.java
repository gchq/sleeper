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

package sleeper.clients.admin;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.admin.testutils.AdminClientMockStoreBase;
import sleeper.clients.admin.testutils.RunAdminClient;
import sleeper.clients.status.report.ingest.task.IngestTaskStatusReportTestHelper;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.tracker.ingest.job.IngestJobStatus;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.task.IngestTaskStatus;
import sleeper.core.tracker.ingest.task.IngestTaskTracker;
import sleeper.task.common.QueueMessageCount;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.DISPLAY_MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.INGEST_JOB_STATUS_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.INGEST_STATUS_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.INGEST_TASK_STATUS_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.INGEST_TRACKER_NOT_ENABLED_MESSAGE;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.JOB_QUERY_ALL_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.JOB_QUERY_DETAILED_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.JOB_QUERY_RANGE_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.JOB_QUERY_REJECTED_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.JOB_QUERY_UNFINISHED_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_RETURN_TO_MAIN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TASK_QUERY_ALL_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TASK_QUERY_UNFINISHED_OPTION;
import static sleeper.clients.testutil.TestConsoleInput.CONFIRM_PROMPT;
import static sleeper.clients.util.console.ConsoleOutput.CLEAR_CONSOLE;
import static sleeper.core.properties.instance.IngestProperty.INGEST_TRACKER_ENABLED;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.record.process.ProcessRunTestData.startedRun;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestJobStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestStartedStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.rejectedRun;
import static sleeper.task.common.InMemoryQueueMessageCounts.visibleMessages;

class IngestStatusReportScreenTest extends AdminClientMockStoreBase {
    @DisplayName("Ingest job status report")
    @Nested
    class IngestJobStatusReport {
        private static final String INGEST_JOB_QUEUE_URL = "test-ingest-queue";
        private final IngestJobTracker tracker = mock(IngestJobTracker.class);
        private final InstanceProperties instanceProperties = createInstancePropertiesWithJobQueueUrl();
        private final TableProperties tableProperties = createValidTableProperties(instanceProperties, "test-table");
        private final QueueMessageCount.Client queueCounts = visibleMessages(INGEST_JOB_QUEUE_URL, 10);

        @Test
        void shouldRunReportWithQueryTypeAll() throws Exception {
            // Given
            when(tracker.getAllJobs(tableProperties.get(TABLE_ID)))
                    .thenReturn(oneStartedJobStatus());

            // When/Then
            String output = runIngestJobStatusReport()
                    .enterPrompts(JOB_QUERY_ALL_OPTION, CONFIRM_PROMPT)
                    .exitGetOutput();
            assertThat(output)
                    .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                    .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                    .contains("" +
                            "Ingest Job Status Report\n" +
                            "------------------------\n" +
                            "Jobs waiting in ingest queue (excluded from report): 10\n" +
                            "Total jobs waiting across all queues: 10\n" +
                            "Total jobs in report: 1\n" +
                            "Total jobs in progress: 1\n" +
                            "Total jobs finished: 0");

            verifyWithNumberOfPromptsBeforeExit(4);
        }

        @Test
        void shouldRunReportWithQueryTypeUnfinished() throws Exception {
            // Given
            when(tracker.getUnfinishedJobs(tableProperties.get(TABLE_ID)))
                    .thenReturn(oneStartedJobStatus());

            // When/Then
            String output = runIngestJobStatusReport()
                    .enterPrompts(JOB_QUERY_UNFINISHED_OPTION, CONFIRM_PROMPT)
                    .exitGetOutput();
            assertThat(output)
                    .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                    .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                    .contains("" +
                            "Ingest Job Status Report\n" +
                            "------------------------\n" +
                            "Jobs waiting in ingest queue (excluded from report): 10\n" +
                            "Total jobs waiting across all queues: 10\n" +
                            "Total jobs in report: 1\n" +
                            "Total jobs in progress: 1\n" +
                            "-");

            verifyWithNumberOfPromptsBeforeExit(4);
        }

        @Test
        void shouldRunReportWithQueryTypeDetailed() throws Exception {
            // Given
            when(tracker.getJob("test-job"))
                    .thenReturn(Optional.of(startedJobStatus("test-job")));

            // When/Then
            String output = runIngestJobStatusReport()
                    .enterPrompts(JOB_QUERY_DETAILED_OPTION, "test-job", CONFIRM_PROMPT)
                    .exitGetOutput();
            assertThat(output)
                    .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                    .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                    .contains("" +
                            "Ingest Job Status Report\n" +
                            "------------------------\n" +
                            "Details for job test-job");

            verifyWithNumberOfPromptsBeforeExit(5);
        }

        @Test
        void shouldRunReportWithQueryTypeRange() throws Exception {
            // Given
            when(tracker.getJobsInTimePeriod(tableProperties.get(TABLE_ID),
                    Instant.parse("2023-03-15T14:00:00Z"), Instant.parse("2023-03-15T18:00:00Z")))
                    .thenReturn(oneStartedJobStatus());

            // When/Then
            String output = runIngestJobStatusReport()
                    .enterPrompts(JOB_QUERY_RANGE_OPTION,
                            "20230315140000", "20230315180000", CONFIRM_PROMPT)
                    .exitGetOutput();
            assertThat(output)
                    .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                    .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                    .contains("" +
                            "Ingest Job Status Report\n" +
                            "------------------------\n" +
                            "Jobs waiting in ingest queue (excluded from report): 10\n" +
                            "Total jobs waiting across all queues: 10\n" +
                            "Total jobs in defined range: 1\n");

            verifyWithNumberOfPromptsBeforeExit(6);
        }

        @Test
        void shouldRunReportWithQueryTypeRejected() throws Exception {
            // Given
            when(tracker.getInvalidJobs())
                    .thenReturn(oneRejectedJobStatus());

            // When/Then
            String output = runIngestJobStatusReport()
                    .enterPrompts(JOB_QUERY_REJECTED_OPTION, CONFIRM_PROMPT)
                    .exitGetOutput();
            assertThat(output)
                    .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                    .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                    .contains("" +
                            "Ingest Job Status Report\n" +
                            "------------------------\n" +
                            "Jobs waiting in ingest queue (excluded from report): 10\n" +
                            "Total jobs waiting across all queues: 10\n" +
                            "Total jobs rejected: 1");

            verifyWithNumberOfPromptsBeforeExit(4);
        }

        private RunAdminClient runIngestJobStatusReport() {
            setInstanceProperties(instanceProperties, tableProperties);
            return runClient().enterPrompts(INGEST_STATUS_REPORT_OPTION,
                    INGEST_JOB_STATUS_REPORT_OPTION, "test-table")
                    .queueClient(queueCounts).tracker(tracker);
        }

        private List<IngestJobStatus> oneStartedJobStatus() {
            return List.of(startedJobStatus("test-job"));
        }

        private List<IngestJobStatus> oneRejectedJobStatus() {
            return List.of(ingestJobStatus("test-job",
                    rejectedRun("test-job", "{}", Instant.parse("2023-07-05T11:59:00Z"),
                            "Test reason")));
        }

        private IngestJobStatus startedJobStatus(String jobId) {
            return ingestJobStatus(jobId, startedRun("test-task",
                    ingestStartedStatus(Instant.parse("2023-03-15T17:52:12.001Z"), 1)));
        }

        private InstanceProperties createInstancePropertiesWithJobQueueUrl() {
            InstanceProperties properties = createValidInstanceProperties();
            properties.set(CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL, INGEST_JOB_QUEUE_URL);
            return properties;
        }
    }

    @DisplayName("Ingest task status report")
    @Nested
    class IngestTaskStatusReport {
        private final IngestTaskTracker tracker = mock(IngestTaskTracker.class);

        private List<IngestTaskStatus> exampleTaskStatuses() {
            return List.of(
                    IngestTaskStatusReportTestHelper.startedTask("test-task", "2023-03-15T17:52:12.001Z"));
        }

        @Test
        void shouldRunIngestTaskStatusReportWithQueryTypeAll() throws Exception {
            // Given
            when(tracker.getAllTasks())
                    .thenReturn(exampleTaskStatuses());

            // When/Then
            String output = runIngestTaskStatusReport()
                    .enterPrompts(TASK_QUERY_ALL_OPTION, CONFIRM_PROMPT)
                    .exitGetOutput();
            assertThat(output)
                    .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                    .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                    .contains("" +
                            "Ingest Task Status Report\n" +
                            "-------------------------\n" +
                            "Total tasks: 1\n" +
                            "Total tasks in progress: 1\n" +
                            "Total tasks finished: 0");

            verifyWithNumberOfPromptsBeforeExit(3);
        }

        @Test
        void shouldRunIngestTaskStatusReportWithQueryTypeUnfinished() throws Exception {
            // Given
            when(tracker.getTasksInProgress())
                    .thenReturn(exampleTaskStatuses());

            // When/Then
            String output = runIngestTaskStatusReport()
                    .enterPrompts(TASK_QUERY_UNFINISHED_OPTION, CONFIRM_PROMPT)
                    .exitGetOutput();
            assertThat(output)
                    .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                    .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                    .contains("" +
                            "Ingest Task Status Report\n" +
                            "-------------------------\n" +
                            "Total tasks in progress: 1\n");

            verifyWithNumberOfPromptsBeforeExit(3);
        }

        private RunAdminClient runIngestTaskStatusReport() {
            setInstanceProperties(createValidInstanceProperties());
            return runClient().enterPrompts(INGEST_STATUS_REPORT_OPTION,
                    INGEST_TASK_STATUS_REPORT_OPTION)
                    .tracker(tracker);
        }
    }

    @Test
    void shouldReturnToMainMenuIfIngestTrackerNotEnabled() throws Exception {
        // Given
        InstanceProperties properties = createValidInstanceProperties();
        properties.set(INGEST_TRACKER_ENABLED, "false");
        setInstanceProperties(properties);

        // When
        String output = runClient()
                .enterPrompts(INGEST_STATUS_REPORT_OPTION, CONFIRM_PROMPT)
                .exitGetOutput();

        // Then
        assertThat(output)
                .isEqualTo(DISPLAY_MAIN_SCREEN +
                        INGEST_TRACKER_NOT_ENABLED_MESSAGE +
                        PROMPT_RETURN_TO_MAIN + DISPLAY_MAIN_SCREEN);
        verifyWithNumberOfPromptsBeforeExit(1);
    }
}
