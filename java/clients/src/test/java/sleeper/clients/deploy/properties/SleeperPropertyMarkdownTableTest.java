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
package sleeper.clients.deploy.properties;

import org.junit.jupiter.api.Test;

import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.clients.util.tablewriter.TableWriter;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.CommonProperty;
import sleeper.core.properties.table.TableProperty;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.testutil.ClientTestUtils.example;

public class SleeperPropertyMarkdownTableTest {
    ToStringConsoleOutput output = new ToStringConsoleOutput();

    @Test
    void shouldWriteTable() throws IOException {
        // Given
        TableWriter writer = SleeperPropertyMarkdownTable.createTableWriterForUserDefinedProperties(List.of(
                CommonProperty.JARS_BUCKET,
                CommonProperty.FILE_SYSTEM));

        // When
        writer.write(output.getPrintStream());

        // Then
        assertThat(output).hasToString(example("reports/table/property.txt"));
    }

    @Test
    void shouldWriteTableWithLongDescriptionEntry() throws IOException {
        // Given
        TableWriter writer = SleeperPropertyMarkdownTable.createTableWriterForUserDefinedProperties(List.of(
                CommonProperty.TASK_RUNNER_LAMBDA_MEMORY_IN_MB,
                CommonProperty.TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS));

        // When
        writer.write(output.getPrintStream());

        // Then
        assertThat(output).hasToString(example("reports/table/propertyLong.txt"));
    }

    @Test
    void shouldWriteTableWithCdkDefinedProperties() throws IOException {
        // Given
        TableWriter writer = SleeperPropertyMarkdownTable.createTableWriterForCdkDefinedProperties(List.of(
                CdkDefinedInstanceProperty.VERSION,
                CdkDefinedInstanceProperty.ADMIN_ROLE_ARN));

        // When
        writer.write(output.getPrintStream());

        // Then
        assertThat(output).hasToString(example("reports/table/propertyCdk.txt"));
    }

    @Test
    void shouldWriteTableWithTableProperties() throws IOException {
        // Given
        TableWriter writer = SleeperPropertyMarkdownTable.createTableWriterForTableProperties(List.of(
                TableProperty.TABLE_NAME,
                TableProperty.TABLE_ONLINE));

        // When
        writer.write(output.getPrintStream());

        // Then
        assertThat(output).hasToString(example("reports/table/propertyTable.txt"));
    }

}
