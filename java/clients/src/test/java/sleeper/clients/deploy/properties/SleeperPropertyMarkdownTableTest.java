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
import sleeper.core.properties.SleeperProperty;
import sleeper.core.properties.instance.CommonProperty;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.testutil.ClientTestUtils.example;

public class SleeperPropertyMarkdownTableTest {

    @Test
    void shouldCreateValidPropertiesTableWhenGivenAStreamOfProperties() throws IOException {
        // Given
        ToStringConsoleOutput output = new ToStringConsoleOutput();

        List<SleeperProperty> propertyList = List.of(CommonProperty.JARS_BUCKET,
                CommonProperty.FILE_SYSTEM);

        TableWriter writer = SleeperPropertyMarkdownTable.generateTableBuildForGroup(propertyList.stream());

        // When
        writer.write(output.getPrintStream());
        // Then
        assertThat(output).hasToString(example("reports/table/property.txt"));
    }

    @Test
    void shouldCreateMarkdownTableWithLongDescriptionEntry() throws IOException {
        // Given
        ToStringConsoleOutput output = new ToStringConsoleOutput();

        List<SleeperProperty> propertyList = List.of(CommonProperty.TASK_RUNNER_LAMBDA_MEMORY_IN_MB,
                CommonProperty.TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS);

        TableWriter writer = SleeperPropertyMarkdownTable.generateTableBuildForGroup(propertyList.stream());

        // When
        writer.write(output.getPrintStream());
        // Then
        assertThat(output).hasToString(example("reports/table/propertyLong.txt"));
    }

}
