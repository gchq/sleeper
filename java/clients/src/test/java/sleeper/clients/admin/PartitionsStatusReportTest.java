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
import org.mockito.InOrder;
import org.mockito.Mockito;

import sleeper.clients.admin.testutils.AdminClientMockStoreBase;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.statestore.inmemory.StateStoreTestBuilder;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.EXIT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PARTITION_STATUS_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_RETURN_TO_MAIN;
import static sleeper.console.ConsoleOutput.CLEAR_CONSOLE;
import static sleeper.status.report.partitions.PartitionStatusReportTestHelper.createPartitionsBuilder;

public class PartitionsStatusReportTest extends AdminClientMockStoreBase {
    @Test
    void shouldRunPartitionStatusReportIfStateStoreExists() {
        // Given
        InstanceProperties properties = createValidInstanceProperties();
        setInstanceProperties(properties);
        setStateStore(properties, createValidTableProperties(properties, "test-table"),
                StateStoreTestBuilder.from(createPartitionsBuilder()
                                .leavesWithSplits(Arrays.asList("A", "B"), List.of("aaa"))
                                .parentJoining("parent", "A", "B"))
                        .singleFileInEachLeafPartitionWithRecords(5)
                        .buildStateStore());

        in.enterNextPrompts(PARTITION_STATUS_REPORT_OPTION, "test-table", EXIT_OPTION);

        // When
        String output = runClientGetOutput();
        assertThat(output)
                .startsWith(CLEAR_CONSOLE + MAIN_SCREEN)
                .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                .contains("Partitions Status Report:")
                .contains("There are 3 partitions (2 leaf partitions")
                .contains("There are 0 leaf partitions that need splitting")
                .contains("Split threshold is 1000000000 records");
        confirmAndVerifyNoMoreInteractions();
    }

    private void confirmAndVerifyNoMoreInteractions() {
        InOrder order = Mockito.inOrder(in.mock);
        order.verify(in.mock, times(2)).promptLine(any());
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }
}
