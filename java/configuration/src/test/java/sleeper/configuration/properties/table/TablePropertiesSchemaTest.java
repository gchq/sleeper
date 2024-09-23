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
package sleeper.configuration.properties.table;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.PropertiesUtils.loadProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

class TablePropertiesSchemaTest {

    @Test
    void shouldFailToLoadFromStringIfTableSchemaIsAbsent() {
        // Given
        Properties input = loadProperties(
                "sleeper.table.name=myTable\n");
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        // When / Then
        assertThatThrownBy(() -> tableProperties.resetAndValidate(input))
                .hasMessage("Property sleeper.table.schema was invalid. It was unset.");
    }

    @Test
    void shouldFailToLoadFromStringIfTableSchemaIsInvalid() {
        // Given
        Properties input = loadProperties("" +
                "sleeper.table.name=myTable\n" +
                "sleeper.table.schema={}\n");
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        // When / Then
        assertThatThrownBy(() -> tableProperties.resetAndValidate(input))
                .hasMessage("Must have at least one row key field");
    }

    @Test
    void shouldConstructFromPropertiesWithNoValidationWhenSchemaIsAbsent() {
        // Given
        String input = "" +
                "sleeper.table.name=myTable\n";
        InstanceProperties instanceProperties = new InstanceProperties();
        Properties properties = loadProperties(input);

        // When / Then
        TableProperties tableProperties = new TableProperties(instanceProperties, properties);

        // Then
        assertThat(tableProperties)
                .extracting(p -> p.get(TABLE_NAME), TableProperties::getSchema)
                .containsExactly("myTable", null);
    }

    @Test
    void shouldLoadAndValidateSuccessfullyIfTableSchemaIsInPropertyInConstructor() {
        // Given
        String input = "" +
                "sleeper.table.name=myTable\n" +
                "sleeper.table.schema={\"rowKeyFields\":[{\"name\":\"key\",\"type\":\"StringType\"}]}";
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        Properties properties = loadProperties(input);

        // When
        TableProperties tableProperties = TableProperties.createAndValidate(new InstanceProperties(), properties);

        // Then
        assertThat(tableProperties.get(TABLE_NAME)).isEqualTo("myTable");
        assertThat(tableProperties.getSchema()).isEqualTo(schema);
    }

    @Test
    void shouldLoadFromStringAndSetSchemaSeparately() {
        // Given
        String input = "" +
                "sleeper.table.name=myTable\n";
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();

        // When
        TableProperties tableProperties = new TableProperties(new InstanceProperties(), loadProperties(input));
        tableProperties.setSchema(schema);

        // Then
        assertThat(tableProperties.get(TABLE_NAME)).isEqualTo("myTable");
        assertThat(tableProperties.getSchema()).isEqualTo(schema);
    }

    @Test
    void shouldFailToLoadAndValidateIfMandatoryPropertyIsMissing() {
        // Given
        String input = "" +
                "sleeper.table.schema={\"rowKeyFields\":[{\"name\":\"key\",\"type\":\"StringType\"}]}\n";

        // When
        InstanceProperties instanceProperties = new InstanceProperties();
        Properties properties = loadProperties(input);

        // Then
        assertThatThrownBy(() -> TableProperties.createAndValidate(instanceProperties, properties))
                .hasMessage("Property sleeper.table.name was invalid. It was unset.");
    }
}
