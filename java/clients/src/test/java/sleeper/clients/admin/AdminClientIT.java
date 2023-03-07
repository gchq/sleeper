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

import sleeper.clients.admin.testutils.AdminClientITBase;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.EXIT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.INSTANCE_PROPERTY_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_RETURN_TO_MAIN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_NAMES_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_PROPERTY_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_PROPERTY_REPORT_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.UPDATE_PROPERTY_ENTER_TABLE_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.UPDATE_PROPERTY_ENTER_VALUE_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.UPDATE_PROPERTY_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.UPDATE_PROPERTY_SCREEN;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.console.ConsoleOutput.CLEAR_CONSOLE;

class AdminClientIT extends AdminClientITBase {

    @Test
    void shouldPrintInstancePropertyReportWhenChosen() throws Exception {
        // Given
        createValidInstanceProperties().saveToS3(s3);
        in.enterNextPrompts(INSTANCE_PROPERTY_REPORT_OPTION, EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output)
                .startsWith(CLEAR_CONSOLE + MAIN_SCREEN)
                .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                .contains("Instance Property Report")
                .contains("sleeper.account=1234567890\n");
    }

    @Test
    void shouldPrintTableNamesReportWhenChosen() throws Exception {
        // Given
        InstanceProperties instanceProperties = createValidInstanceProperties();
        instanceProperties.saveToS3(s3);
        TableProperties tableProperties1 = createValidTableProperties(instanceProperties, "test-table-1");
        tableProperties1.saveToS3(s3);
        TableProperties tableProperties2 = createValidTableProperties(instanceProperties, "test-table-2");
        tableProperties2.saveToS3(s3);
        in.enterNextPrompts(TABLE_NAMES_REPORT_OPTION, EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN + "\n\n" +
                " Table Names Report \n" +
                " -------------------------\n" +
                "test-table-1\n" +
                "test-table-2\n" +
                PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN);
    }

    @Test
    void shouldPrintTablePropertyReportWhenChosen() throws Exception {
        // Given
        InstanceProperties instanceProperties = createValidInstanceProperties();
        instanceProperties.saveToS3(s3);
        TableProperties tableProperties = createValidTableProperties(instanceProperties);
        tableProperties.saveToS3(s3);
        in.enterNextPrompts(TABLE_PROPERTY_REPORT_OPTION, tableProperties.get(TABLE_NAME), EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output)
                .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE + TABLE_PROPERTY_REPORT_SCREEN)
                .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                .contains("Table Property Report")
                .contains("sleeper.table.name=test-table\n");
    }

    @Test
    void shouldUpdateInstancePropertyWhenNameAndValueEntered() throws Exception {
        // Given
        InstanceProperties instanceProperties = createValidInstanceProperties();
        instanceProperties.set(MAXIMUM_CONNECTIONS_TO_S3, "2");
        instanceProperties.saveToS3(s3);
        in.enterNextPrompts(UPDATE_PROPERTY_OPTION, "sleeper.s3.max-connections", "2",
                INSTANCE_PROPERTY_REPORT_OPTION, EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).startsWith(CLEAR_CONSOLE + MAIN_SCREEN
                        + CLEAR_CONSOLE + UPDATE_PROPERTY_SCREEN
                        + CLEAR_CONSOLE + UPDATE_PROPERTY_ENTER_VALUE_SCREEN
                        + "sleeper.s3.max-connections has been updated to 2\n"
                        + PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                .contains("Instance Property Report")
                .contains("sleeper.s3.max-connections=2\n");

        InstanceProperties instancePropertiesAfter = new InstanceProperties();
        instancePropertiesAfter.loadFromS3(s3, instanceProperties.get(CONFIG_BUCKET));
        assertThat(instancePropertiesAfter.getInt(MAXIMUM_CONNECTIONS_TO_S3)).isEqualTo(2);
    }

    @Test
    void shouldUpdateTablePropertyWhenNameValueAndTableEntered() throws Exception {
        // Given
        InstanceProperties instanceProperties = createValidInstanceProperties();
        instanceProperties.saveToS3(s3);
        TableProperties tableProperties = createValidTableProperties(instanceProperties);
        tableProperties.set(ITERATOR_CLASS_NAME, "BeforeIteratorClass");
        tableProperties.saveToS3(s3);
        in.enterNextPrompts(UPDATE_PROPERTY_OPTION,
                "sleeper.table.iterator.class.name", "AfterIteratorClass", TABLE_NAME_VALUE,
                TABLE_PROPERTY_REPORT_OPTION, tableProperties.get(TABLE_NAME), EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).startsWith(CLEAR_CONSOLE + MAIN_SCREEN
                        + CLEAR_CONSOLE + UPDATE_PROPERTY_SCREEN
                        + CLEAR_CONSOLE + UPDATE_PROPERTY_ENTER_VALUE_SCREEN
                        + CLEAR_CONSOLE + UPDATE_PROPERTY_ENTER_TABLE_SCREEN
                        + "sleeper.table.iterator.class.name has been updated to AfterIteratorClass\n"
                        + PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                .contains("Table Property Report")
                .contains("sleeper.table.iterator.class.name=AfterIteratorClass\n");

        TableProperties tablePropertiesAfter = new TableProperties(instanceProperties);
        tablePropertiesAfter.loadFromS3(s3, tableProperties.get(TABLE_NAME));
        assertThat(tablePropertiesAfter.get(ITERATOR_CLASS_NAME)).isEqualTo("AfterIteratorClass");
    }
}
