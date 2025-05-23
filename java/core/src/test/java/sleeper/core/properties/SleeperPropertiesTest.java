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
package sleeper.core.properties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.model.EmrInstanceArchitecture;
import sleeper.core.properties.model.IngestQueue;

import java.io.PrintWriter;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.PropertiesUtils.loadProperties;
import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.USER_JARS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ENDPOINT_CHECK;
import static sleeper.core.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.IngestProperty.INGEST_SOURCE_BUCKET;
import static sleeper.core.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_INSTANCE_ARCHITECTURE;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_BATCHER_INGEST_QUEUE;
import static sleeper.core.properties.model.EmrInstanceArchitecture.ARM64;
import static sleeper.core.properties.model.EmrInstanceArchitecture.X86_64;
import static sleeper.core.properties.model.IngestQueue.BULK_IMPORT_PERSISTENT_EMR;
import static sleeper.core.properties.table.TableProperty.PAGE_SIZE;

class SleeperPropertiesTest {

    @Nested
    @DisplayName("Handle values")
    class HandleValues {

        @Test
        void shouldFindUnsetValueIsUnset() {
            // Given
            TestSleeperProperties testSleeperProperties = new TestSleeperProperties();

            // When / Then
            assertThat(testSleeperProperties.isSet(PAGE_SIZE)).isFalse();
        }

        @Test
        void shouldFindSetValueIsSet() {
            // Given
            TestSleeperProperties testSleeperProperties = new TestSleeperProperties();
            testSleeperProperties.set(PAGE_SIZE, "5");

            // When / Then
            assertThat(testSleeperProperties.isSet(PAGE_SIZE)).isTrue();
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
    }

    @Nested
    @DisplayName("Handle numeric types")
    class HandleNumbers {

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
        void shouldReturnNullIntegerObjectWhenValueSetToNull() {

            // Given
            TestSleeperProperties testSleeperProperties = new TestSleeperProperties();

            // When // Then
            assertThat(testSleeperProperties.getIntOrNull(GARBAGE_COLLECTOR_LAMBDA_CONCURRENCY_MAXIMUM)).isNull();
        }

        @Test
        void shouldFailToReadWhenPropertySetToValueThatIsNotAnInt() {
            TestSleeperProperties testSleeperProperties = new TestSleeperProperties();

            // When
            testSleeperProperties.set(GARBAGE_COLLECTOR_LAMBDA_CONCURRENCY_RESERVED, "Forty Two");

            // Then
            assertThatThrownBy(() -> testSleeperProperties.getIntOrNull(GARBAGE_COLLECTOR_LAMBDA_CONCURRENCY_RESERVED))
                    .isInstanceOf(NumberFormatException.class)
                    .hasMessageContaining("Forty Two");
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
    }

    @Nested
    @DisplayName("Check equality")
    class CheckEquality {

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
    }

    @Nested
    @DisplayName("Handle list properties")
    class HandleListProperties {

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
        void shouldSetList() {
            // Given
            TestSleeperProperties testSleeperProperties = new TestSleeperProperties();

            // When
            testSleeperProperties.setList(OPTIONAL_STACKS, List.of("a", "b", "c"));

            // Then
            assertThat(testSleeperProperties.get(OPTIONAL_STACKS)).isEqualTo("a,b,c");
        }

        @Test
        void shouldSetEmptyListWhenPropertyHasNoDefaultValue() {
            // Given
            TestSleeperProperties testSleeperProperties = new TestSleeperProperties();
            testSleeperProperties.setList(INGEST_SOURCE_BUCKET, List.of("test-bucket"));

            // When
            testSleeperProperties.setList(INGEST_SOURCE_BUCKET, List.of());

            // Then
            assertThat(testSleeperProperties.get(INGEST_SOURCE_BUCKET)).isNull();
            assertThat(testSleeperProperties.getList(INGEST_SOURCE_BUCKET)).isEmpty();
        }

        @Test
        void shouldSetEmptyListWhenPropertyHasDefaultValueAndIsSetToAllowEmptyValue() {
            // Given
            TestSleeperProperties testSleeperProperties = new TestSleeperProperties();

            // When
            testSleeperProperties.setList(OPTIONAL_STACKS, List.of());

            // Then
            assertThat(testSleeperProperties.get(OPTIONAL_STACKS)).isEmpty();
            assertThat(testSleeperProperties.getList(OPTIONAL_STACKS)).isEmpty();
            assertThat(testSleeperProperties.isSet(OPTIONAL_STACKS)).isTrue();
        }

        @Test
        void shouldReadEmptyListFromString() {
            // Given
            TestSleeperProperties testSleeperProperties = new TestSleeperProperties(
                    loadProperties("sleeper.optional.stacks="));

            // When / Then
            assertThat(testSleeperProperties.get(OPTIONAL_STACKS)).isEmpty();
            assertThat(testSleeperProperties.getList(OPTIONAL_STACKS)).isEmpty();
        }

        @Test
        void shouldReadEmptyListFromStringWithSpace() {
            // Given
            TestSleeperProperties testSleeperProperties = new TestSleeperProperties(
                    loadProperties("sleeper.optional.stacks= \n"));

            // When / Then
            assertThat(testSleeperProperties.get(OPTIONAL_STACKS)).isEmpty();
            assertThat(testSleeperProperties.getList(OPTIONAL_STACKS)).isEmpty();
        }

        @Test
        void shouldAddToList() {
            // Given
            TestSleeperProperties testSleeperProperties = new TestSleeperProperties();
            testSleeperProperties.setList(OPTIONAL_STACKS, List.of("a", "b"));

            // When
            testSleeperProperties.addToListIfMissing(OPTIONAL_STACKS, List.of("c", "d"));

            // Then
            assertThat(testSleeperProperties.get(OPTIONAL_STACKS)).isEqualTo("a,b,c,d");
        }

        @Test
        void shouldAddToUnsetListWhenPropertyHasDefaultValue() {
            // Given
            TestSleeperProperties testSleeperProperties = new TestSleeperProperties();

            // When
            testSleeperProperties.addToListIfMissing(OPTIONAL_STACKS, List.of("a", "b"));

            // Then
            assertThat(testSleeperProperties.get(OPTIONAL_STACKS))
                    .isEqualTo(OPTIONAL_STACKS.getDefaultValue() + ",a,b");
        }

        @Test
        void shouldAddToEmptyListWhenPropertyHasDefaultValueAndIsSetToAllowEmptyValue() {
            // Given
            TestSleeperProperties testSleeperProperties = new TestSleeperProperties();
            testSleeperProperties.setList(OPTIONAL_STACKS, List.of());

            // When
            testSleeperProperties.addToListIfMissing(OPTIONAL_STACKS, List.of("a", "b"));

            // Then
            assertThat(testSleeperProperties.get(OPTIONAL_STACKS))
                    .isEqualTo("a,b");
        }

        @Test
        void shouldAddToUnsetListWhenPropertyHasNoDefaultValue() {
            // Given
            TestSleeperProperties testSleeperProperties = new TestSleeperProperties();

            // When
            testSleeperProperties.addToListIfMissing(INGEST_SOURCE_BUCKET, List.of("a", "b"));

            // Then
            assertThat(testSleeperProperties.get(INGEST_SOURCE_BUCKET))
                    .isEqualTo("a,b");
        }

        @Test
        void shouldNotAddToListWhenAlreadyInList() {
            // Given
            TestSleeperProperties testSleeperProperties = new TestSleeperProperties();
            testSleeperProperties.addToListIfMissing(INGEST_SOURCE_BUCKET, List.of("a", "b"));

            // When
            testSleeperProperties.addToListIfMissing(INGEST_SOURCE_BUCKET, List.of("a", "c"));

            // Then
            assertThat(testSleeperProperties.get(INGEST_SOURCE_BUCKET))
                    .isEqualTo("a,b,c");
        }

        @Test
        void shouldNotChangeExistingValuesWhenAddingToList() {
            // Given
            TestSleeperProperties testSleeperProperties = new TestSleeperProperties();
            testSleeperProperties.addToListIfMissing(INGEST_SOURCE_BUCKET, List.of("something", "old", "something"));

            // When
            testSleeperProperties.addToListIfMissing(INGEST_SOURCE_BUCKET, List.of("something", "new"));

            // Then
            assertThat(testSleeperProperties.get(INGEST_SOURCE_BUCKET))
                    .isEqualTo("something,old,something,new");
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
    }

    @Nested
    @DisplayName("Handle enum properties")
    class HandleEnumProperties {
        @Test
        void shouldReadEnumPropertyAsList() {
            // Given
            TestSleeperProperties testSleeperProperties = new TestSleeperProperties();
            testSleeperProperties.set(BULK_IMPORT_PERSISTENT_EMR_INSTANCE_ARCHITECTURE, "x86_64,arm64");

            // When / Then
            assertThat(testSleeperProperties.streamEnumList(
                    BULK_IMPORT_PERSISTENT_EMR_INSTANCE_ARCHITECTURE, EmrInstanceArchitecture.class))
                    .containsExactly(X86_64, ARM64);
        }

        @Test
        void shouldReadEnumPropertyAsSingleValue() {
            // Given
            TestSleeperProperties testSleeperProperties = new TestSleeperProperties();
            testSleeperProperties.set(DEFAULT_INGEST_BATCHER_INGEST_QUEUE, "bulk_import_persistent_emr");

            // When / Then
            assertThat(testSleeperProperties.getEnumValue(DEFAULT_INGEST_BATCHER_INGEST_QUEUE, IngestQueue.class))
                    .isEqualTo(BULK_IMPORT_PERSISTENT_EMR);
        }

        @Test
        void shouldSetEnumPropertyAsSingleValue() {
            // Given
            TestSleeperProperties testSleeperProperties = new TestSleeperProperties();
            testSleeperProperties.setEnum(DEFAULT_INGEST_BATCHER_INGEST_QUEUE, BULK_IMPORT_PERSISTENT_EMR);

            // When / Then
            assertThat(testSleeperProperties.get(DEFAULT_INGEST_BATCHER_INGEST_QUEUE))
                    .isEqualTo("bulk_import_persistent_emr");
        }

        @Test
        void shouldSetEnumPropertyAsList() {
            // Given
            TestSleeperProperties testSleeperProperties = new TestSleeperProperties();
            testSleeperProperties.setEnumList(BULK_IMPORT_PERSISTENT_EMR_INSTANCE_ARCHITECTURE, List.of(X86_64, ARM64));

            // When / Then
            assertThat(testSleeperProperties.get(BULK_IMPORT_PERSISTENT_EMR_INSTANCE_ARCHITECTURE))
                    .isEqualTo("x86_64,arm64");
        }
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
        public String get(SleeperProperty property) {
            return compute(property, value -> value != null ? value : property.getDefaultValue());
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
