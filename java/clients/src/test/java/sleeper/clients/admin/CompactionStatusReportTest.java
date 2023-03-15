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

import sleeper.clients.admin.testutils.AdminClientMockStoreBase;
import sleeper.configuration.properties.InstanceProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.COMPACTION_JOB_STATUS_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.COMPACTION_STATUS_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.EXIT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.JOB_QUERY_ALL_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_RETURN_TO_MAIN;
import static sleeper.console.ConsoleOutput.CLEAR_CONSOLE;

class CompactionStatusReportTest extends AdminClientMockStoreBase {
    @Nested
    @DisplayName("Compaction job status report")
    class CompactionJobStatusReport {
        @Test
        void shouldRunCompactionJobStatusReportWithQueryTypeAll() {
            // Given
            createCompactionStatusStore();
            in.enterNextPrompts(COMPACTION_STATUS_REPORT_OPTION,
                    COMPACTION_JOB_STATUS_REPORT_OPTION, "test-table", JOB_QUERY_ALL_OPTION,
                    EXIT_OPTION);

            String output = runClientGetOutput();
            assertThat(output)
                    .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                    .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                    .contains("Compaction Job Status Report")
                    .contains("Total jobs: 1")
                    .contains("Total standard jobs: 1");
        }
    }

    private void createCompactionStatusStore() {
        InstanceProperties properties = createValidInstanceProperties();
        setInstanceProperties(properties);
        setCompactionStatusStore(properties,
                createValidTableProperties(properties, "test-table"));
    }
}
