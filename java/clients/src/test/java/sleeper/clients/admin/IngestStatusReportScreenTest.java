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

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import sleeper.clients.admin.testutils.AdminClientMockStoreBase;
import sleeper.clients.admin.testutils.RunAdminClient;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.status.IngestJobStatus;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.task.IngestTaskStatus;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.status.report.ingest.task.IngestTaskStatusReportTestHelper;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.INGEST_JOB_STATUS_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.INGEST_STATUS_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.INGEST_TASK_STATUS_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.JOB_QUERY_ALL_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.JOB_QUERY_DETAILED_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.JOB_QUERY_RANGE_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.JOB_QUERY_UNFINISHED_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_RETURN_TO_MAIN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TASK_QUERY_ALL_OPTION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.console.ConsoleOutput.CLEAR_CONSOLE;
import static sleeper.console.TestConsoleInput.CONFIRM_PROMPT;
import static sleeper.ingest.job.status.IngestJobStatusTestData.startedIngestJob;

public class IngestStatusReportScreenTest extends AdminClientMockStoreBase {
    @DisplayName("Ingest job status report")
    @Nested
    class IngestJobStatusReport {
        private final IngestJobStatusStore ingestJobStatusStore = mock(IngestJobStatusStore.class);

        @Test
        void shouldRunIngestJobStatusReportWithQueryTypeAll() throws Exception {
            // Given
            createIngestJobStatusStore();
            createSqsClient();
            when(ingestJobStatusStore.getAllJobs("test-table"))
                    .thenReturn(exampleJobStatuses());

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
                            "Total jobs waiting in queue (excluded from report): 10\n" +
                            "Total jobs in progress: 1\n" +
                            "Total jobs finished: 0");

            verifyWithNumberOfInvocations(4);
        }

        @Test
        void shouldRunIngestJobStatusReportWithQueryTypeUnfinished() throws Exception {
            // Given
            createIngestJobStatusStore();
            createSqsClient();
            when(ingestJobStatusStore.getUnfinishedJobs("test-table"))
                    .thenReturn(exampleJobStatuses());

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
                            "Total jobs waiting in queue (excluded from report): 10\n" +
                            "Total jobs in progress: 1\n" +
                            "-");

            verifyWithNumberOfInvocations(4);
        }

        @Test
        void shouldRunIngestJobStatusReportWithQueryTypeDetailed() throws Exception {
            // Given
            createIngestJobStatusStore();
            createSqsClient();
            IngestJobStatus exampleJob = exampleJobStatuses().get(0);
            when(ingestJobStatusStore.getJob("test-job"))
                    .thenReturn(Optional.of(exampleJob));

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

            verifyWithNumberOfInvocations(5);
        }

        @Test
        void shouldRunIngestJobStatusReportWithQueryTypeRange() throws Exception {
            // Given
            createIngestJobStatusStore();
            createSqsClient();
            when(ingestJobStatusStore.getJobsInTimePeriod("test-table",
                    Instant.parse("2023-03-15T14:00:00Z"), Instant.parse("2023-03-15T18:00:00Z")))
                    .thenReturn(exampleJobStatuses());

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
                            "Total jobs waiting in queue (excluded from report): 10\n" +
                            "Total jobs in defined range: 1\n");

            verifyWithNumberOfInvocations(6);
        }

        private RunAdminClient runIngestJobStatusReport() {
            return runClient().enterPrompts(INGEST_STATUS_REPORT_OPTION,
                    INGEST_JOB_STATUS_REPORT_OPTION, "test-table");
        }

        private void createIngestJobStatusStore() {
            InstanceProperties properties = createValidInstanceProperties();
            setInstanceProperties(properties, createValidTableProperties(properties, "test-table"));
            when(store.loadIngestJobStatusStore(properties.get(ID)))
                    .thenReturn(ingestJobStatusStore);
        }

        private void createSqsClient() {
            when(store.getSqsClient())
                    .thenReturn(mock(AmazonSQS.class));
            when(store.getSqsClient().getQueueAttributes(any()))
                    .thenReturn(new GetQueueAttributesResult()
                            .withAttributes(Map.of(
                                    "ApproximateNumberOfMessages", "10",
                                    "ApproximateNumberOfMessagesNotVisible", "15")));
        }

        private List<IngestJobStatus> exampleJobStatuses() {
            return List.of(
                    startedIngestJob(IngestJob.builder().id("test-job").files("test.parquet").build(),
                            "test-task", Instant.parse("2023-03-15T17:52:12.001Z")));
        }
    }

    @DisplayName("Ingest task status report")
    @Nested
    class IngestTaskStatusReport {
        private final IngestTaskStatusStore ingestTaskStatusStore = mock(IngestTaskStatusStore.class);

        @Test
        void shouldRunIngestTaskStatusReportWithQueryTypeAll() throws Exception {
            // Given
            createIngestTaskStatusStore();
            when(ingestTaskStatusStore.getAllTasks())
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

            verifyWithNumberOfInvocations(3);
        }

        private RunAdminClient runIngestTaskStatusReport() {
            return runClient().enterPrompts(INGEST_STATUS_REPORT_OPTION,
                    INGEST_TASK_STATUS_REPORT_OPTION);
        }

        private void createIngestTaskStatusStore() {
            InstanceProperties properties = createValidInstanceProperties();
            setInstanceProperties(properties);
            when(store.loadIngestTaskStatusStore(properties.get(ID)))
                    .thenReturn(ingestTaskStatusStore);
        }

        private List<IngestTaskStatus> exampleTaskStatuses() {
            return List.of(
                    IngestTaskStatusReportTestHelper.startedTask("test-task", "2023-03-15T17:52:12.001Z"));
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
