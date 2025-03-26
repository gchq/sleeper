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
import sleeper.core.properties.SleeperProperty;
import sleeper.core.properties.instance.CommonProperty;
import sleeper.core.properties.instance.InstanceProperty;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.testutil.ClientTestUtils.example;

public class TableWriterPropertyHelperTest {

    @Test
    void shouldCreateMarkdownTableWithProvidedInstanceProperties() throws IOException {
        // Given
        TableWriterFactory.Builder factoryBuilder = TableWriterFactory.builder().structure(TableStructure.MARKDOWN_FORMAT);
        factoryBuilder.addFields(TableWriterPropertyHelper.getMarkdownFields());

        TableWriterFactory factory = factoryBuilder.build();
        ToStringConsoleOutput output = new ToStringConsoleOutput();

        InstanceProperty property1 = CommonProperty.JARS_BUCKET;
        InstanceProperty property2 = CommonProperty.FILE_SYSTEM;

        // When
        factory.tableBuilder()
                .row(TableWriterPropertyHelper.generatePropertyDetails(factoryBuilder.getFields(), property1))
                .row(TableWriterPropertyHelper.generatePropertyDetails(factoryBuilder.getFields(), property2))
                .build().write(output.getPrintStream());
        // Then
        assertThat(output).hasToString(example("reports/table/property.txt"));
    }

    @Test
    void shouldCreateMarkdownTableWithLongDescriptionEntry() throws IOException {
        // Given
        TableStructure structure = TableStructure.MARKDOWN_FORMAT;
        TableWriterFactory.Builder factoryBuilder = TableWriterFactory.builder().structure(structure);
        factoryBuilder.addFields(TableWriterPropertyHelper.getMarkdownFields());

        TableWriterFactory factory = factoryBuilder.build();
        ToStringConsoleOutput output = new ToStringConsoleOutput();

        InstanceProperty property1 = CommonProperty.TASK_RUNNER_LAMBDA_MEMORY_IN_MB;
        InstanceProperty property2 = CommonProperty.TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS;

        // When
        factory.tableBuilder()
                .row(TableWriterPropertyHelper.generatePropertyDetails(factoryBuilder.getFields(), property1))
                .row(TableWriterPropertyHelper.generatePropertyDetails(factoryBuilder.getFields(), property2))
                .build().write(output.getPrintStream());
        // Then
        assertThat(output).hasToString(example("reports/table/propertyLong.txt"));

    }

    @Test
    void shouldCreateValidPropertiesTableWhenGivenAStreamOfProperties() throws IOException {
        // Given
        TableStructure structure = TableStructure.MARKDOWN_FORMAT;
        TableWriterFactory.Builder factoryBuilder = TableWriterFactory.builder().structure(structure);
        factoryBuilder.addFields(TableWriterPropertyHelper.getMarkdownFields());

        ToStringConsoleOutput output = new ToStringConsoleOutput();

        List<SleeperProperty> propertyList = List.of(CommonProperty.TASK_RUNNER_LAMBDA_MEMORY_IN_MB,
                CommonProperty.TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS);

        TableWriter writer = TableWriterPropertyHelper.generateTableBuildForGroup(propertyList.stream());

        // When
        writer.write(output.getPrintStream());
        // Then
        assertThat(output).hasToString(example("reports/table/propertyLong.txt"));
    }

}
