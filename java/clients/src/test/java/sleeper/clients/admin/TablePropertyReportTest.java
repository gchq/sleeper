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


        assertThat(output)
                // Check some set table property values are present in the output
                .contains("# A unique name identifying this table.\n" +
                        "sleeper.table.name: test-table\n")
                .contains("# The size of the row group in the Parquet files - defaults to the value in the instance properties.\n" +
                        "sleeper.table.rowgroup.size: 8388608\n")
                .contains("# The schema representing the structure of this table.\n" +
                        "sleeper.table.schema: " +
                        "{\"rowKeyFields\":[{\"name\":\"key\",\"type\":\"StringType\"}]," +
                        "\"sortKeyFields\":[]," +
                        "\"valueFields\":[{\"name\":\"value\",\"type\":\"StringType\"}]}\n")
                // Check a property with multi-line description and custom line breaks
                .contains("# The number of files to read in a compaction job. Note that the state store must support atomic\n" +
                        "# updates for this many files.\n" +
                        "# The DynamoDBStateStore must be able to atomically apply 2 updates to create the output files for a\n" +
                        "# splitting compaction, and 2 updates for each input file to mark them as ready for garbage\n" +
                        "# collection. There's a limit of 100 atomic updates, which equates to 48 files in a compaction.\n" +
                        "# See also: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html\n" +
                        "# (NB This does not apply to splitting jobs which will run even if there is only 1 file.)\n" +
                        "sleeper.table.compaction.files.batch.size: 11");

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
                        "# When this is changed, existing files will retain their encryption status. Further compactions may\n" +
                        "# apply the new encryption status for that data.\n" +
                        "# See also: https://docs.aws.amazon.com/AmazonS3/latest/userguide/default-bucket-encryption.html\n" +
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
                .contains("# The following table properties relate to the definition of data inside a table.\n\n")
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
        assertThat(output.indexOf("The following table properties relate to the definition of data inside a table."))
                .isLessThan(output.indexOf("The following table properties relate to compactions."));
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
