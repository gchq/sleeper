/*
 * Copyright 2022-2025 Crown Copyright
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
import org.mockito.InOrder;
import org.mockito.Mockito;

import sleeper.clients.admin.testutils.AdminClientMockStoreBase;
import sleeper.core.table.TableStatus;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_RETURN_TO_MAIN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_NAMES_REPORT_OPTION;
import static sleeper.clients.testutil.TestConsoleInput.CONFIRM_PROMPT;
import static sleeper.clients.util.console.ConsoleOutput.CLEAR_CONSOLE;
import static sleeper.core.table.TableStatus.uniqueIdAndName;

class TableNamesReportTest extends AdminClientMockStoreBase {

    public static String generateExpectedTableNamesOutput(List<String> tableNames) {
        return "\n\n" +
                "Table Names\n" +
                "----------------------------------\n" + tableNames.stream().map(s -> s + "\n").collect(Collectors.joining(""));
    }

    public static String generateExpectedTableNamesOutput(String... tableNames) {
        return generateExpectedTableNamesOutput(Arrays.asList(tableNames));
    }

    @Test
    void shouldPrintTableNamesReportWhenChosen() throws Exception {
        // Given
        setInstanceTables(createValidInstanceProperties(),
                onlineTable("test-table-1-id", "test-table-1"),
                onlineTable("test-table-2-id", "test-table-2"));

        // When
        String output = runClient()
                .enterPrompts(TABLE_NAMES_REPORT_OPTION, CONFIRM_PROMPT)
                .exitGetOutput();

        // Then
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN + "\n\n" +
                "Table Names\n" +
                "----------------------------------\n" +
                "test-table-1\n" +
                "test-table-2\n" +
                PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN);

        InOrder order = Mockito.inOrder(in.mock);
        order.verify(in.mock).promptLine(any());
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }

    @Test
    void shouldPrintOnlineAndOfflineTableNames() throws Exception {
        // Given
        Stream<TableStatus> tables = Stream.of(
                onlineTable("test-table-1-id", "test-table-1"),
                onlineTable("test-table-2-id", "test-table-2"),
                offlineTable("test-table-3-id", "test-table-3"),
                offlineTable("test-table-4-id", "test-table-4"));
        setInstanceTables(createValidInstanceProperties(), tables);

        // When
        String output = runClient()
                .enterPrompts(TABLE_NAMES_REPORT_OPTION, CONFIRM_PROMPT)
                .exitGetOutput();

        // Then
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN + "\n\n" +
                "Table Names\n" +
                "----------------------------------\n" +
                "test-table-1\n" +
                "test-table-2\n" +
                "test-table-3 (offline)\n" +
                "test-table-4 (offline)\n" +
                PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN);

        InOrder order = Mockito.inOrder(in.mock);
        order.verify(in.mock).promptLine(any());
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }

    private static TableStatus onlineTable(String tableId, String tableName) {
        return uniqueIdAndName(tableId, tableName, true);
    }

    private static TableStatus offlineTable(String tableId, String tableName) {
        return uniqueIdAndName(tableId, tableName, false);
    }
}
