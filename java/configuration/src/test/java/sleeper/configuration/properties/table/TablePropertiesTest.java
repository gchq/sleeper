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
package sleeper.configuration.properties.table;

import org.junit.Test;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SleeperProperty;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_PAGE_SIZE;
import static sleeper.configuration.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class TablePropertiesTest {

    @Test
    public void shouldThrowExceptionIfCompressionCodecIsInvalidOnInit() {
        // Given
        String input = "" +
                "sleeper.table.name=myTable\n" +
                "sleeper.table.schema={}\n" +
                "sleeper.table.compression.codec=madeUp";
        // When / Then
        try {
            new TableProperties(new InstanceProperties()).loadFromString(input);
        } catch (Exception e) {
            assertEquals("Property sleeper.table.compression.codec was invalid. It was \"madeUp\"", e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionIfTableNameIsAbsentOnInit() {
        // Given
        String input = "" +
                "sleeper.table.schema={}\n";
        // When / Then
        try {
            new TableProperties(new InstanceProperties()).loadFromString(input);
        } catch (Exception e) {
            assertEquals("Property sleeper.table.name was invalid. It was \"null\"", e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionIfTableSchemaIsAbsentOnInit() {
        // Given
        String input = "" +
                "sleeper.table.name=myTable\n";
        // When / Then
        try {
            new TableProperties(new InstanceProperties()).loadFromString(input);
        } catch (Exception e) {
            assertEquals("Property sleeper.table.schema was invalid. It was \"null\"", e.getMessage());
        }
    }

    @Test
    public void shouldDefaultToInstancePropertiesValueWhenConfigured() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(DEFAULT_PAGE_SIZE, "20");

        // When
        TableProperties tableProperties = new TableProperties(instanceProperties);

        // Then
        assertEquals("20", tableProperties.get(PAGE_SIZE));
    }

    @Test
    public void shouldDefaultToAnotherTablePropertyIfConfigured() {
        // Given
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.set(TABLE_NAME, "id");

        // When
        ITableProperty defaultingProperty = new ITableProperty() {
            @Override
            public SleeperProperty getDefaultProperty() {
                return TABLE_NAME;
            }

            @Override
            public String getPropertyName() {
                return "made.up";
            }

            @Override
            public String getDefaultValue() {
                return null;
            }
        };

        // Then
        assertEquals("id", tableProperties.get(defaultingProperty));
    }

    @Test
    public void shouldThrowRuntimeExceptionIfConfiguredWithNonInstanceOrNonTableProperty() {
        // Given
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.set(TABLE_NAME, "id");

        // When
        SleeperProperty sleeperProperty = new SleeperProperty() {
            @Override
            public String getPropertyName() {
                return null;
            }

            @Override
            public String getDefaultValue() {
                return null;
            }
        };

        ITableProperty defaultingProperty = new ITableProperty() {
            @Override
            public SleeperProperty getDefaultProperty() {
                return sleeperProperty;
            }

            @Override
            public String getPropertyName() {
                return "made.up";
            }

            @Override
            public String getDefaultValue() {
                return null;
            }
        };

        // Then
        try {
            tableProperties.get(defaultingProperty);
            fail("Exception expected");
        } catch (RuntimeException e) {
            assertEquals("Unable to process SleeperProperty, should have either been null, an " +
                    "instance property or a table property", e.getMessage());
        }
    }

    @Test
    public void shouldReturnTrueForEqualityWhenInstancePropertiesAreEqual() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();

        // When
        TableProperties tableProperties = new TableProperties(instanceProperties);
        TableProperties duplicateProperties = new TableProperties(instanceProperties);

        // Then
        assertEquals(tableProperties, duplicateProperties);
    }

    @Test
    public void shouldReturnFalseForEqualityWhenPropertiesAreDifferent() {
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
        assertNotEquals(tableProperties, differentTableProperties);
    }
}
