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

import sleeper.configuration.properties.DummySleeperProperty;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SleeperProperty;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_PAGE_SIZE;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

class TablePropertiesTest {

    @Test
    void shouldThrowExceptionIfCompressionCodecIsInvalidOnInit() {
        // Given
        String input = "" +
                "sleeper.table.name=myTable\n" +
                "sleeper.table.schema={\"rowKeyFields\":[{\"name\":\"key\",\"type\":\"StringType\"}]}\n" +
                "sleeper.table.compression.codec=madeUp";
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        // When / Then
        assertThatThrownBy(() -> tableProperties.loadFromString(input))
                .hasMessage("Property sleeper.table.compression.codec was invalid. It was \"madeUp\"");
    }

    @Test
    void shouldThrowExceptionIfTableNameIsAbsentOnInit() {
        // Given
        String input = "" +
                "sleeper.table.schema={\"rowKeyFields\":[{\"name\":\"key\",\"type\":\"StringType\"}]}\n";
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        // When / Then
        assertThatThrownBy(() -> tableProperties.loadFromString(input))
                .hasMessage("Property sleeper.table.name was invalid. It was \"null\"");
    }

    @Test
    void shouldDefaultToInstancePropertiesValueWhenConfigured() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(DEFAULT_PAGE_SIZE, "20");

        // When
        TableProperties tableProperties = new TableProperties(instanceProperties);

        // Then
        assertThat(tableProperties.get(PAGE_SIZE)).isEqualTo("20");
    }

    @Test
    void shouldDefaultToAnotherTablePropertyIfConfigured() {
        // Given
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.set(TABLE_NAME, "id");

        // When
        TableProperty defaultingProperty = DummyTableProperty.defaultedFrom(TABLE_NAME);

        // Then
        assertThat(tableProperties.get(defaultingProperty)).isEqualTo("id");
    }

    @Test
    void shouldThrowRuntimeExceptionIfConfiguredWithNonInstanceOrNonTableProperty() {
        // Given
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.set(TABLE_NAME, "id");

        // When
        SleeperProperty sleeperProperty = new DummySleeperProperty();
        TableProperty defaultingProperty = DummyTableProperty.defaultedFrom(sleeperProperty);

        // Then
        assertThatThrownBy(() -> tableProperties.get(defaultingProperty))
                .hasMessage("Unable to process SleeperProperty, should have either been null, an " +
                        "instance property or a table property");
    }

    @Test
    void shouldReturnTrueForEqualityWhenInstancePropertiesAreEqual() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();

        // When
        TableProperties tableProperties = new TableProperties(instanceProperties);
        TableProperties duplicateProperties = new TableProperties(instanceProperties);

        // Then
        assertThat(duplicateProperties).isEqualTo(tableProperties);
    }

    @Test
    void shouldReturnFalseForEqualityWhenPropertiesAreDifferent() {
        // Given
        Properties properties = new Properties();
        properties.setProperty("a", "b");
        InstanceProperties instanceProperties = new InstanceProperties(properties);

        Properties differentProperties = new Properties();
        properties.setProperty("a", "c");
        InstanceProperties differentInstanceProperties = new InstanceProperties(differentProperties);

        // When
        TableProperties tableProperties = new TableProperties(instanceProperties);
        TableProperties differentTableProperties = new TableProperties(differentInstanceProperties);

        // Then
        assertThat(differentTableProperties).isNotEqualTo(tableProperties);
    }

    @Test
    void shouldFailValidationIfCompactionFilesBatchSizeTooLargeForDynamoDBStateStore() {
        // Given
        InstanceProperties instanceProperties = createTestInstanceProperties();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
        tableProperties.set(TableProperty.STATESTORE_CLASSNAME, "sleeper.statestore.dynamodb.DynamoDBStateStore");
        tableProperties.setNumber(TableProperty.COMPACTION_FILES_BATCH_SIZE, 49);

        // When/Then
        assertThatThrownBy(tableProperties::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Property sleeper.table.compaction.files.batch.size was invalid. " +
                        "It was \"49\"");
    }

    @Test
    void shouldPassValidationIfCompactionFilesBatchSizeTooLargeForDynamoDBStateStoreButS3StateStoreChosen() {
        // Given
        InstanceProperties instanceProperties = createTestInstanceProperties();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
        tableProperties.set(TableProperty.STATESTORE_CLASSNAME, "sleeper.statestore.s3.S3StateStore");
        tableProperties.setNumber(TableProperty.COMPACTION_FILES_BATCH_SIZE, 49);

        // When/Then
        assertThatCode(tableProperties::validate).doesNotThrowAnyException();
    }

    @Test
    void shouldKeepValidationPredicateSameAsOnDefaultProperty() {
        assertThat(TableProperty.getAll().stream()
                .filter(property -> property.getDefaultProperty() != null)
                .filter(property -> property.getDefaultProperty()
                        .validationPredicate() != property.validationPredicate()))
                .isEmpty();
    }

    @Test
    void shouldKeepCDKDeploymentTriggerSameAsOnDefaultProperty() {
        assertThat(TableProperty.getAll().stream()
                .filter(property -> property.getDefaultProperty() != null)
                .filter(property -> property.getDefaultProperty()
                        .isRunCDKDeployWhenChanged() != property.isRunCDKDeployWhenChanged()))
                .isEmpty();
    }

    @Test
    void shouldGetUnknownPropertyValues() throws IOException {
        // Given
        InstanceProperties instanceProperties = createTestInstanceProperties();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
        tableProperties.loadFromString("unknown.property=123");

        // When / Then
        assertThat(tableProperties.getUnknownProperties())
                .containsExactly(Map.entry("unknown.property", "123"));
    }
}
