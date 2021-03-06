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
package sleeper.configuration.properties;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Properties;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import org.junit.Test;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.USER_JARS;
import static sleeper.configuration.properties.table.TableProperty.PAGE_SIZE;

public class SleeperPropertiesTest {

    @Test
    public void shouldGetNumericValueAsString() {
        // Given
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties();

        // When
        testSleeperProperties.set(PAGE_SIZE, "5");

        // Then
        assertEquals("5", testSleeperProperties.get(PAGE_SIZE));
    }

    @Test
    public void shouldSetNumericValueAsNumber() {
        // Given
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties();

        // When
        testSleeperProperties.setNumber(PAGE_SIZE, 2.504);

        // Then
        assertEquals("2.504", testSleeperProperties.get(PAGE_SIZE));
    }

    @Test
    public void shouldBeAbleToRetrieveNumericalNumberAsALong() {
        // Given
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties();

        // When
        testSleeperProperties.set(PAGE_SIZE, "5");

        // Then
        assertEquals(new Long(5), testSleeperProperties.getLong(PAGE_SIZE));
    }

    @Test
    public void shouldBeAbleToRetrieveNumericalNumberAsAnInteger() {
        // Given
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties();

        // When
        testSleeperProperties.set(PAGE_SIZE, "5");

        // Then
        assertEquals(new Integer(5), testSleeperProperties.getInt(PAGE_SIZE));
    }

    @Test
    public void shouldDoNothingWhenSettingNullNumber() {
        // Given
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties();
        testSleeperProperties.set(PAGE_SIZE, "5");

        // When
        testSleeperProperties.setNumber(PAGE_SIZE, null);

        // Then
        assertEquals(new Integer(5), testSleeperProperties.getInt(PAGE_SIZE));
    }

    @Test
    public void shouldDoNothingWhenSettingNullString() {
        // Given
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties();
        testSleeperProperties.set(PAGE_SIZE, "5");

        // When
        testSleeperProperties.set(PAGE_SIZE, null);

        // Then
        assertEquals(new Integer(5), testSleeperProperties.getInt(PAGE_SIZE));
    }

    @Test
    public void shouldReturnTrueForEqualityWhenPropertiesAreEqual() {
        // Given
        Properties properties = new Properties();
        properties.setProperty("a", "b");

        // When
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties(properties);
        TestSleeperProperties duplicate = new TestSleeperProperties(properties);

        // Then
        assertEquals(testSleeperProperties, duplicate);
    }

    @Test
    public void shouldReturnFalseForEqualityWhenPropertiesAreDifferent() {
        // Given
        Properties properties = new Properties();
        properties.setProperty("a", "b");

        Properties differentProperties = new Properties();
        properties.setProperty("a", "c");

        // When
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties(properties);
        TestSleeperProperties duplicate = new TestSleeperProperties(differentProperties);

        // Then
        assertNotEquals(testSleeperProperties, duplicate);
    }

    @Test
    public void shouldParsePropertyAsList() {
        // Given
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties();
        testSleeperProperties.set(OPTIONAL_STACKS, "a,b,c");

        // When
        List<String> list = testSleeperProperties.getList(OPTIONAL_STACKS);

        // Then
        assertEquals(Lists.newArrayList("a", "b", "c"), list);
    }

    @Test
    public void shouldReturnNullIfListIsNullOrUnset() {
        // Given
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties();

        // When
        List<String> list = testSleeperProperties.getList(USER_JARS);

        // Then
        assertNull(list);
    }

    private static class TestSleeperProperties extends SleeperProperties<SleeperProperty> {

        public TestSleeperProperties() {
            this(new Properties());
        }
        
        public TestSleeperProperties(Properties properties) {
            super(properties);
        }

        @Override
        protected void validate() {
            // do nothing
        }
    }
}
