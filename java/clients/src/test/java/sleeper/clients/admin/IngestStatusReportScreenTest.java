/*
 * Copyright 2022-2026 Crown Copyright
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

import sleeper.clients.admin.testutils.AdminClientInMemoryTestBase;
import sleeper.clients.admin.testutils.RunAdminClient;
import sleeper.clients.report.ingest.task.IngestTaskStatusReportTestHelper;
import sleeper.common.task.QueueMessageCount;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.tracker.ingest.job.InMemoryIngestJobTracker;
import sleeper.core.tracker.ingest.job.update.IngestJobStartedEvent;
import sleeper.core.tracker.ingest.task.InMemoryIngestTaskTracker;
import sleeper.core.tracker.ingest.task.IngestTaskStatus;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
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
import static sleeper.common.task.InMemoryQueueMessageCounts.visibleMessages;
import static sleeper.core.properties.instance.IngestProperty.INGEST_TRACKER_ENABLED;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.tracker.ingest.job.update.IngestJobValidatedEvent.ingestJobRejected;

class IngestStatusReportScreenTest extends AdminClientInMemoryTestBase {
    @DisplayName("Ingest job status report")
    @Nested
    class IngestJobStatusReport {
        private static final String INGEST_JOB_QUEUE_URL = "test-ingest-queue";
        private final InMemoryIngestJobTracker tracker = new InMemoryIngestJobTracker();
        private final InstanceProperties instanceProperties = createInstancePropertiesWithJobQueueUrl();
        private final TableProperties tableProperties = createValidTableProperties(instanceProperties, "test-table");
        private final QueueMessageCount.Client queueCounts = visibleMessages(INGEST_JOB_QUEUE_URL, 10);

        @Test
        void shouldRunReportWithQueryTypeAll() throws Exception {
            // Given
            startExampleJob();

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
            startExampleJob();

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
            startExampleJob();

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
            startExampleJob();

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
            tracker.jobValidated(ingestJobRejected("test-job", "{}",
                    Instant.parse("2023-07-05T11:59:00Z"), "Test reason"));

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

        private void startExampleJob() {
            tracker.jobStarted(IngestJobStartedEvent.builder()
                    .jobId("test-job")
                    .tableId(tableProperties.get(TABLE_ID))
                    .jobRunId("test-run")
                    .taskId("test-task")
                    .startTime(Instant.parse("2023-03-15T17:52:12.001Z"))
                    .fileCount(1)
                    .build());
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
        private final InMemoryIngestTaskTracker tracker = new InMemoryIngestTaskTracker();

        private List<IngestTaskStatus> exampleTaskStatuses() {
            return List.of(
                    IngestTaskStatusReportTestHelper.startedTask("test-task", "2023-03-15T17:52:12.001Z"));
        }

        @Test
        void shouldRunIngestTaskStatusReportWithQueryTypeAll() throws Exception {
            // Given
            exampleTaskStatuses().forEach(tracker::taskStarted);

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
            exampleTaskStatuses().forEach(tracker::taskStarted);

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
