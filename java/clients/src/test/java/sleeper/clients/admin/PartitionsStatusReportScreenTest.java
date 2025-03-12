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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import sleeper.clients.admin.testutils.AdminClientMockStoreBase;
import sleeper.core.partition.PartitionsBuilderSplitsFirst;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PARTITION_STATUS_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_RETURN_TO_MAIN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.RETURN_TO_MAIN_SCREEN_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_SELECT_SCREEN;
import static sleeper.clients.testutil.TestConsoleInput.CONFIRM_PROMPT;
import static sleeper.clients.util.console.ConsoleOutput.CLEAR_CONSOLE;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

class PartitionsStatusReportScreenTest extends AdminClientMockStoreBase {

    private final Schema schema = schemaWithKey("key", new StringType());
    private final InstanceProperties instanceProperties = createValidInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final StateStore stateStore = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());

    @BeforeEach
    void setUp() {
        setInstanceProperties(instanceProperties, tableProperties);
        setStateStoreForTable("test-table", stateStore);
    }

    @Test
    void shouldRunPartitionStatusReport() throws Exception {
        // Given
        update(stateStore).initialise(PartitionsBuilderSplitsFirst.leavesWithSplits(
                schema, List.of("A", "B"), List.of("aaa"))
                .parentJoining("parent", "A", "B").buildList());
        update(stateStore).addFiles(FileReferenceFactory.from(stateStore).singleFileInEachLeafPartitionWithRecords(5).toList());

        // When
        String output = runClient()
                .enterPrompts(PARTITION_STATUS_REPORT_OPTION,
                        "test-table", CONFIRM_PROMPT)
                .exitGetOutput();

        // Then
        assertThat(output)
                .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE + TABLE_SELECT_SCREEN)
                .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                .contains("Partitions Status Report:")
                .contains("There are 3 partitions (2 leaf partitions")
                .contains("There are 0 leaf partitions that will be split")
                .contains("Split threshold is 1000000000 records");
        confirmAndVerifyNoMoreInteractions();
    }

    @Test
    void shouldReturnToMenuWhenOnTableNameScreen() throws Exception {
        // When
        String output = runClient()
                .enterPrompts(PARTITION_STATUS_REPORT_OPTION,
                        RETURN_TO_MAIN_SCREEN_OPTION)
                .exitGetOutput();

        // Then
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE +
                TABLE_SELECT_SCREEN + CLEAR_CONSOLE + MAIN_SCREEN);
        InOrder order = Mockito.inOrder(in.mock);
        order.verify(in.mock, times(3)).promptLine(any());
        order.verifyNoMoreInteractions();
    }

    private void confirmAndVerifyNoMoreInteractions() {
        InOrder order = Mockito.inOrder(in.mock);
        order.verify(in.mock, times(2)).promptLine(any());
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }
}
