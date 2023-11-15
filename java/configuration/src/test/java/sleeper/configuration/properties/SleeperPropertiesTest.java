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
package sleeper.configuration.properties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.format.SleeperPropertiesPrettyPrinter;
import sleeper.configuration.properties.instance.SleeperProperty;

import java.io.ByteArrayInputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.instance.CommonProperty.USER_JARS;
import static sleeper.configuration.properties.instance.CommonProperty.VPC_ENDPOINT_CHECK;
import static sleeper.configuration.properties.table.TableProperty.PAGE_SIZE;

class SleeperPropertiesTest {

    @Test
    void shouldGetNumericValueAsString() {
        // Given
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties();

        // When
        testSleeperProperties.set(PAGE_SIZE, "5");

        // Then
        assertThat(testSleeperProperties.get(PAGE_SIZE)).isEqualTo("5");
    }

    @Test
    void shouldSetNumericValueAsNumber() {
        // Given
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties();

        // When
        testSleeperProperties.setNumber(PAGE_SIZE, 2.504);

        // Then
        assertThat(testSleeperProperties.get(PAGE_SIZE)).isEqualTo("2.504");
    }

    @Test
    void shouldBeAbleToRetrieveNumericalNumberAsALong() {
        // Given
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties();

        // When
        testSleeperProperties.set(PAGE_SIZE, "5");

        // Then
        assertThat(testSleeperProperties.getLong(PAGE_SIZE)).isEqualTo(Long.valueOf(5L));
    }

    @Test
    void shouldBeAbleToRetrieveNumericalNumberAsAnInteger() {
        // Given
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties();

        // When
        testSleeperProperties.set(PAGE_SIZE, "5");

        // Then
        assertThat(testSleeperProperties.getInt(PAGE_SIZE)).isEqualTo(Integer.valueOf(5));
    }

    @Test
    void shouldDoNothingWhenSettingNullNumber() {
        // Given
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties();
        testSleeperProperties.set(PAGE_SIZE, "5");

        // When
        testSleeperProperties.setNumber(PAGE_SIZE, null);

        // Then
        assertThat(testSleeperProperties.getInt(PAGE_SIZE)).isEqualTo(Integer.valueOf(5));
    }

    @Test
    void shouldDoNothingWhenSettingNullString() {
        // Given
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties();
        testSleeperProperties.set(PAGE_SIZE, "5");

        // When
        testSleeperProperties.set(PAGE_SIZE, null);

        // Then
        assertThat(testSleeperProperties.getInt(PAGE_SIZE)).isEqualTo(Integer.valueOf(5));
    }

    @Test
    void shouldReturnTrueForEqualityWhenPropertiesAreEqual() {
        // Given
        Properties properties = new Properties();
        properties.setProperty("a", "b");

        // When
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties(properties);
        TestSleeperProperties duplicate = new TestSleeperProperties(properties);

        // Then
        assertThat(duplicate).isEqualTo(testSleeperProperties);
    }

    @Test
    void shouldReturnFalseForEqualityWhenPropertiesAreDifferent() {
        // Given
        Properties properties = new Properties();
        properties.setProperty("a", "b");

        Properties differentProperties = new Properties();
        properties.setProperty("a", "c");

        // When
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties(properties);
        TestSleeperProperties duplicate = new TestSleeperProperties(differentProperties);

        // Then
        assertThat(duplicate).isNotEqualTo(testSleeperProperties);
    }

    @Test
    void shouldParsePropertyAsList() {
        // Given
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties();
        testSleeperProperties.set(OPTIONAL_STACKS, "a,b,c");

        // When
        List<String> list = testSleeperProperties.getList(OPTIONAL_STACKS);

        // Then
        assertThat(list).containsExactly("a", "b", "c");
    }

    @Test
    void shouldReturnEmptyListIfListIsNullOrUnset() {
        // Given
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties();

        // When
        List<String> list = testSleeperProperties.getList(USER_JARS);

        // Then
        assertThat(list).isEmpty();
    }

    @Test
    void shouldResetPropertiesWhenLoadingFromString() {
        // Given
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties();
        testSleeperProperties.setNumber(PAGE_SIZE, 123);

        // When
        testSleeperProperties.loadFromString("a=value-a");

        // Then
        assertThat(testSleeperProperties.isSet(PAGE_SIZE)).isFalse();
    }

    @Test
    void shouldResetPropertiesWhenLoadingFromStream() {
        // Given
        TestSleeperProperties testSleeperProperties = new TestSleeperProperties();
        testSleeperProperties.setNumber(PAGE_SIZE, 123);

        // When
        testSleeperProperties.load(new ByteArrayInputStream("a=value-a".getBytes()));

        // Then
        assertThat(testSleeperProperties.isSet(PAGE_SIZE)).isFalse();
    }

    @Nested
    @DisplayName("Convert empty strings")
    class ConvertEmptyString {
        @Test
        void shouldGetNullWhenPropertyIsSetToEmptyString() {
            // Given
            TestSleeperProperties sleeperProperties = new TestSleeperProperties();
            sleeperProperties.set(ACCOUNT, "");

            // When
            String value = sleeperProperties.get(ACCOUNT);

            // Then
            assertThat(value).isNull();
        }

        @Test
        void shouldGetDefaultValueWhenPropertyWithDefaultValueIsSetToEmptyString() {
            // Given
            TestSleeperProperties sleeperProperties = new TestSleeperProperties();
            sleeperProperties.set(VPC_ENDPOINT_CHECK, "");

            // When
            boolean value = sleeperProperties.getBoolean(VPC_ENDPOINT_CHECK);

            // Then
            assertThat(value).isTrue();
        }

        @Test
        void shouldTreatEmptyStringAsUnsetValue() {
            // Given
            TestSleeperProperties sleeperProperties = new TestSleeperProperties();
            sleeperProperties.set(ACCOUNT, "");

            // When / Then
            assertThat(sleeperProperties.isSet(ACCOUNT)).isFalse();
        }

        @Test
        void shouldReturnEmptyListIfListPropertyIsSetToEmptyString() {
            // Given
            TestSleeperProperties testSleeperProperties = new TestSleeperProperties();
            testSleeperProperties.set(SUBNETS, "");

            // When
            List<String> list = testSleeperProperties.getList(SUBNETS);

            // Then
            assertThat(list).isEmpty();
        }
    }

    private static class TestSleeperProperties extends SleeperProperties<SleeperProperty> {

        private TestSleeperProperties() {
            this(new Properties());
        }

        private TestSleeperProperties(Properties properties) {
            super(properties);
        }

        @Override
        public SleeperPropertyIndex<SleeperProperty> getPropertiesIndex() {
            return new SleeperPropertyIndex<>();
        }

        @Override
        protected SleeperPropertiesPrettyPrinter<SleeperProperty> getPrettyPrinter(PrintWriter writer) {
            return null;
        }
    }
}
