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
package sleeper.configuration.properties.table;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

class TablePropertiesSchemaTest {

    @Test
    void shouldThrowExceptionIfTableSchemaIsAbsentOnInit() {
        // Given
        String input = "" +
                "sleeper.table.name=myTable\n";
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        // When / Then
        assertThatThrownBy(() -> tableProperties.loadFromString(input))
                .hasMessage("Property sleeper.table.schema was invalid. It was \"null\"");
    }

    @Test
    void shouldLoadSuccessfullyIfTableSchemaIsSetBeforeLoad() throws IOException {
        // Given
        String input = "" +
                "sleeper.table.name=myTable\n";
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();

        // When
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.setSchema(schema);
        tableProperties.loadFromString(input);

        // Then
        assertThat(tableProperties.get(TABLE_NAME)).isEqualTo("myTable");
        assertThat(tableProperties.getSchema()).isEqualTo(schema);
    }
}
