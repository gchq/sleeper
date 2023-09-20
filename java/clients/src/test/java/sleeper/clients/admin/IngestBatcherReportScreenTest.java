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

import org.junit.jupiter.api.Test;

import sleeper.clients.admin.testutils.AdminClientMockStoreBase;
import sleeper.clients.admin.testutils.RunAdminClient;
import sleeper.ingest.batcher.IngestBatcherStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.BATCHER_QUERY_ALL_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.BATCHER_QUERY_PENDING_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.INGEST_BATCHER_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_RETURN_TO_MAIN;
import static sleeper.clients.status.report.ingest.batcher.IngestBatcherReporterTestHelper.multiplePendingFiles;
import static sleeper.clients.status.report.ingest.batcher.IngestBatcherReporterTestHelper.onePendingAndTwoBatchedFiles;
import static sleeper.clients.testutil.TestConsoleInput.CONFIRM_PROMPT;
import static sleeper.clients.util.console.ConsoleOutput.CLEAR_CONSOLE;

public class IngestBatcherReportScreenTest extends AdminClientMockStoreBase {
    private final IngestBatcherStore ingestBatcherStore = mock(IngestBatcherStore.class);

    @Test
    void shouldRunReportForAllFiles() throws Exception {
        // Given
        when(ingestBatcherStore.getAllFilesNewestFirst())
                .thenReturn(onePendingAndTwoBatchedFiles());

        // When/Then
        String output = runIngestBatcherReport()
                .enterPrompts(BATCHER_QUERY_ALL_OPTION, CONFIRM_PROMPT)
                .exitGetOutput();
        assertThat(output)
                .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                .contains("" +
                        "Ingest Batcher Report\n" +
                        "---------------------\n" +
                        "Total pending files: 1\n" +
                        "Total batched files: 2\n" +
                        "----------------------");

        verifyWithNumberOfInvocations(2);
    }

    @Test
    void shouldRunReportForPendingFiles() throws Exception {
        // Given
        when(ingestBatcherStore.getPendingFilesOldestFirst())
                .thenReturn(multiplePendingFiles());

        // When/Then
        String output = runIngestBatcherReport()
                .enterPrompts(BATCHER_QUERY_PENDING_OPTION, CONFIRM_PROMPT)
                .exitGetOutput();
        assertThat(output)
                .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                .contains("" +
                        "Ingest Batcher Report\n" +
                        "---------------------\n" +
                        "Total pending files: 3\n" +
                        "----------------------");

        verifyWithNumberOfInvocations(2);
    }

    private RunAdminClient runIngestBatcherReport() {
        setInstanceProperties(createValidInstanceProperties());
        return runClient().enterPrompts(INGEST_BATCHER_REPORT_OPTION)
                .statusStore(ingestBatcherStore);
    }
}
