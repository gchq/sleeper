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
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;

import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.EXIT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_RETURN_TO_MAIN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.RETURN_TO_MAIN_SCREEN_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_PROPERTY_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_PROPERTY_REPORT_SCREEN;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.console.ConsoleOutput.CLEAR_CONSOLE;

class TablePropertyReportTest extends AdminClientMockStoreBase {
    @Test
    void shouldPrintAllTableProperties() {
        // Given
        createTablePropertiesAndSelectTable();

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output)
                .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE + TABLE_PROPERTY_REPORT_SCREEN)
                .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                .contains("Table Property Report")
                .contains(TableProperty.getAll().stream()
                        .map(TableProperty::getPropertyName)
                        .collect(Collectors.toList()));

        confirmAndVerifyNoMoreInteractions();
    }

    @Test
    void shouldPrintPropertiesAndDescriptions() {
        // Given
        createTablePropertiesAndSelectTable();

        // When
        String output = runClientGetOutput();

        // Then check some set table property values are present in the output
        assertThat(output)
                .contains("# A unique name identifying this table.\n" +
                        "sleeper.table.name: test-table\n")
                .contains("# Whether or not to encrypt the table. If set to \"true\", all data at rest will be encrypted.\n" +
                        "sleeper.table.encrypted: false\n")
                .contains("# The schema representing the structure of this table.\n" +
                        "sleeper.table.schema: " +
                        "{\"rowKeyFields\":[{\"name\":\"key\",\"type\":\"StringType\"}]," +
                        "\"sortKeyFields\":[]," +
                        "\"valueFields\":[{\"name\":\"value\",\"type\":\"StringType\"}]}\n")
                // Check property with multi-line description
                .contains("# The minimum number of files to read in a compaction job. Note that the state store must support\n" +
                        "# atomic updates for this many files. For the DynamoDBStateStore this is 11.\n" +
                        "# (NB This does not apply to splitting jobs which will run even if there is only 1 file.)\n" +
                        "sleeper.table.compaction.files.batch.size: 11")
                // Check property with multi-line description and custom line breaks
                .contains("# A file will not be deleted until this number of seconds have passed after it has been marked as\n" +
                        "# ready for garbage collection. The reason for not deleting files immediately after they have been\n" +
                        "# marked as ready for garbage collection is that they may still be in use by queries. Defaults to the\n" +
                        "# value set in the instance properties.\n" +
                        "sleeper.table.gc.delay.seconds: 600");

        confirmAndVerifyNoMoreInteractions();
    }

    @Test
    void shouldPrintSpacingBetweenProperties() {
        // Given
        createTablePropertiesAndSelectTable();

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output)
                .contains("# Whether or not to encrypt the table. If set to \"true\", all data at rest will be encrypted.\n" +
                        "sleeper.table.encrypted: false\n" +
                        "\n" +
                        "# The size of the row group in the Parquet files - defaults to the value in the instance properties.\n" +
                        "sleeper.table.rowgroup.size: 8388608\n" +
                        "\n" +
                        "# The size of the page in the Parquet files - defaults to the value in the instance properties.\n" +
                        "sleeper.table.page.size: 131072\n");

        confirmAndVerifyNoMoreInteractions();
    }

    @Test
    void shouldPrintPropertyGroupDescriptions() {
        // Given
        createTablePropertiesAndSelectTable();

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output)
                .contains("# The following properties relate to configuring tables.\n\n")
                .contains("# The following table properties relate to the iterator used when reading from the table.\n\n")
                .contains("# The following table properties relate to the split points in the table.\n\n")
                .contains("# The following table properties relate to compactions.\n\n")
                .contains("# The following table properties relate to partition splitting.\n\n")
                .contains("# The following table properties relate to bulk import, i.e. ingesting data using Spark jobs running\n" +
                        "# on EMR or EKS.\n\n")
                .contains("# The following table properties relate to storing and retrieving metadata for tables.\n\n");

        confirmAndVerifyNoMoreInteractions();
    }

    @Test
    void shouldPrintPropertiesInTheCorrectOrder() {
        // Given
        createTablePropertiesAndSelectTable();

        // When
        String output = runClientGetOutput();

        // Then check ordering in the same group
        assertThat(output.indexOf("sleeper.table.name"))
                .isLessThan(output.indexOf("sleeper.table.schema"))
                .isLessThan(output.indexOf("sleeper.table.encrypted"));
        assertThat(output.indexOf("sleeper.table.schema.file"))
                .isLessThan(output.indexOf("sleeper.table.rowgroup.size"));

        // Check ordering in different groups
        assertThat(output.indexOf("sleeper.table.compaction.strategy.sizeratio.ratio"))
                .isLessThan(output.indexOf("sleeper.table.bulk.import.emr.master.instance.type"));
        assertThat(output.indexOf("sleeper.table.data.bucket"))
                .isLessThan(output.indexOf("sleeper.table.splits.key"));

        confirmAndVerifyNoMoreInteractions();
    }

    @Test
    void shouldPrintPropertyGroupsInTheCorrectOrder() {
        // Given
        createTablePropertiesAndSelectTable();

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output.indexOf("The following properties relate to configuring tables."))
                .isLessThan(output.indexOf("The following table properties relate to the iterator used when reading from the table."));
        assertThat(output.indexOf("The following table properties relate to the iterator used when reading from the table."))
                .isLessThan(output.indexOf("The following table properties relate to the split points in the table."));
        assertThat(output.indexOf("The following table properties relate to compactions."))
                .isLessThan(output.indexOf("The following table properties relate to partition splitting."));

        confirmAndVerifyNoMoreInteractions();
    }

    @Test
    void shouldExitWhenChosenOnTablePropertyReportScreen() {
        // Given
        createTableProperties();
        in.enterNextPrompts(TABLE_PROPERTY_REPORT_OPTION, EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN
                + CLEAR_CONSOLE + TABLE_PROPERTY_REPORT_SCREEN);

        verify(in.mock, times(2)).promptLine(any());
        verifyNoMoreInteractions(in.mock);
    }

    @Test
    void shouldReturnToMainScreenWhenChosenOnTablePropertyReportScreen() {
        // Given
        createTableProperties();
        in.enterNextPrompts(TABLE_PROPERTY_REPORT_OPTION, RETURN_TO_MAIN_SCREEN_OPTION, EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN
                + CLEAR_CONSOLE + TABLE_PROPERTY_REPORT_SCREEN
                + CLEAR_CONSOLE + MAIN_SCREEN);

        verify(in.mock, times(3)).promptLine(any());
        verifyNoMoreInteractions(in.mock);
    }

    private void createTablePropertiesAndSelectTable() {
        TableProperties tableProperties = createTableProperties();
        in.enterNextPrompts(TABLE_PROPERTY_REPORT_OPTION, tableProperties.get(TABLE_NAME), EXIT_OPTION);
    }

    private TableProperties createTableProperties() {
        InstanceProperties instanceProperties = createValidInstanceProperties();
        TableProperties tableProperties = createValidTableProperties(instanceProperties);
        setInstanceProperties(instanceProperties, tableProperties);
        return tableProperties;
    }

    private void confirmAndVerifyNoMoreInteractions() {
        InOrder order = Mockito.inOrder(in.mock);
        order.verify(in.mock, times(2)).promptLine(any());
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }
}
