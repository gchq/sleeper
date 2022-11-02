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
package sleeper.clients.admin;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.console.ConsoleOutput;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class AdminClientTest extends AdminClientTestBase {

    private static final String MAIN_SCREEN = ConsoleOutput.CLEAR_CONSOLE + "\n" +
            "ADMINISTRATION COMMAND LINE CLIENT\n" +
            "----------------------------------\n" +
            "\n" +
            "Please select from the below options and hit return:\n" +
            "[0] Exit program\n" +
            "[1] Print Sleeper instance property report\n" +
            "[2] Print Sleeper table names\n" +
            "[3] Print Sleeper table property report\n" +
            "[4] Update an instance or table property\n" +
            "\n";

    private static final String PROMPT_RETURN_TO_MAIN = "Hit enter to return to main screen";

    private static final String TABLE_PROPERTY_REPORT_SCREEN = ConsoleOutput.CLEAR_CONSOLE + "\n" +
            "Which TABLE do you want to check?\n" +
            "\n" +
            "Please enter the TABLE NAME now or use the following options:\n" +
            "[0] Exit program\n" +
            "[1] Return to Main Menu\n" +
            "\n";

    @Test
    public void shouldDisplayMainScreenAndExitWhenChosen() throws Exception {
        // Given
        in.enterNextPrompt("0");

        // When / Then
        assertThat(runClientGetOutput()).isEqualTo(MAIN_SCREEN);
    }

    @Test
    public void shouldPrintInstancePropertyReportWhenChosen() throws Exception {
        // Given
        setInstanceProperties(createValidInstanceProperties());
        in.enterNextPrompts("1", "0");

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).startsWith(MAIN_SCREEN).endsWith(MAIN_SCREEN)
                .contains("Instance Property Report").contains(PROMPT_RETURN_TO_MAIN)
                // Then check some default property values are present in the output, don't check values in case they change
                .contains("sleeper.athena.handler.memory")
                .contains("sleeper.default.page.size")
                .contains("sleeper.query.tracker.ttl.days")
                // Then check some set property values are present in the output
                .contains("sleeper.account: 1234567890")
                .contains("sleeper.log.retention.days: 1")
                .contains("sleeper.tags: name,abc,project,test")
                .contains("sleeper.vpc: aVPC");

        // Then check the ordering of some property names are correct
        assertThat(output.indexOf("sleeper.account"))
                .isLessThan(output.indexOf("sleeper.log.retention.days"))
                .isLessThan(output.indexOf("sleeper.vpc"));
        assertThat(output.indexOf("sleeper.log.retention.days"))
                .isLessThan(output.indexOf("sleeper.vpc"));

        InOrder order = Mockito.inOrder(in.mock);
        order.verify(in.mock).promptLine(any());
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }

    @Test
    public void shouldPrintTablePropertyReportWhenChosen() throws IOException {
        // Given
        InstanceProperties instanceProperties = createValidInstanceProperties();
        TableProperties tableProperties = createValidTableProperties(instanceProperties);
        setInstanceProperties(instanceProperties, tableProperties);
        in.enterNextPrompts("3", tableProperties.get(TABLE_NAME), "0");

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).startsWith(MAIN_SCREEN).contains(TABLE_PROPERTY_REPORT_SCREEN).endsWith(MAIN_SCREEN)
                .contains("Table Property Report").contains(PROMPT_RETURN_TO_MAIN)
                // Then check some default table property values are present in the output, don't check values in case they change
                .contains("sleeper.table.splits.base64.encoded")
                .contains("sleeper.table.statestore.classname")
                .contains("sleeper.table.fs.s3a.readahead.range")
                // Then check some set table property values are present in the output
                .contains("sleeper.table.name: test")
                .contains("sleeper.table.encrypted: false")
                .contains("sleeper.table.schema: " +
                        "{\"rowKeyFields\":[{\"name\":\"key\",\"type\":\"StringType\"}]," +
                        "\"sortKeyFields\":[]," +
                        "\"valueFields\":[{\"name\":\"value\",\"type\":\"StringType\"}]}");

        // Then check the ordering of some property names are correct
        assertThat(output.indexOf("sleeper.table.encrypted"))
                .isLessThan(output.indexOf("sleeper.table.name"))
                .isLessThan(output.indexOf("sleeper.table.schema"));
        assertThat(output.indexOf("sleeper.table.name"))
                .isLessThan(output.indexOf("sleeper.table.schema"));

        InOrder order = Mockito.inOrder(in.mock);
        order.verify(in.mock, times(2)).promptLine(any());
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }

    @Test
    public void shouldExitWhenChoosingTablePropertyReportTable() throws IOException {
        // Given
        InstanceProperties instanceProperties = createValidInstanceProperties();
        TableProperties tableProperties = createValidTableProperties(instanceProperties);
        setInstanceProperties(instanceProperties, tableProperties);
        in.enterNextPrompts("3", "0");

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).startsWith(MAIN_SCREEN)
                .endsWith(TABLE_PROPERTY_REPORT_SCREEN)
                .doesNotContain("Table Property Report")
                .doesNotContain(PROMPT_RETURN_TO_MAIN);

        InOrder order = Mockito.inOrder(in.mock);
        order.verify(in.mock, times(2)).promptLine(any());
        order.verifyNoMoreInteractions();
    }

}
