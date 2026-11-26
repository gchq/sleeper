/*
 * Copyright 2022-2026 Crown Copyright
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.PropertiesUtils.loadProperties;
import static sleeper.core.properties.instance.BulkImportProperty.BULK_IMPORT_JOB_FILE_RETENTION_DAYS;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_OPTIMIZATION;
import static sleeper.core.properties.instance.EMRServerlessProperty.DEFAULT_BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS;
import static sleeper.core.properties.instance.EMRServerlessProperty.DEFAULT_BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS;
import static sleeper.core.properties.instance.EMRServerlessProperty.DEFAULT_BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MIN_EXECUTORS;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MIN_EXECUTORS;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_SEND_TIMEOUT_SECS;
import static sleeper.core.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class SleeperPropertiesValidationTest {
    @DisplayName("Trigger validation")
    @Nested
    class TriggerValidation {

        private InstanceProperties invalidInstanceProperties() {
            InstanceProperties instanceProperties = createTestInstanceProperties();
            instanceProperties.set(MAXIMUM_CONNECTIONS_TO_S3, "-1");
            return instanceProperties;
        }

        private TableProperties invalidTableProperties(InstanceProperties instanceProperties) {
            TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
            tableProperties.set(COMPRESSION_CODEC, "madeUp");
            return tableProperties;
        }

        @Test
        void shouldThrowExceptionOnLoadIfInstancePropertiesValidationFails() {
            // Given
            Properties invalid = invalidInstanceProperties().getProperties();

            // When / Then
            InstanceProperties properties = new InstanceProperties();
            assertThatThrownBy(() -> properties.resetAndValidate(invalid))
                    .isInstanceOf(SleeperPropertiesInvalidException.class);
        }

        @Test
        void shouldThrowExceptionOnLoadIfTablePropertiesValidationFails() {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            Properties invalid = invalidTableProperties(instanceProperties).getProperties();

            // When / Then
            TableProperties properties = new TableProperties(instanceProperties);
            assertThatThrownBy(() -> properties.resetAndValidate(invalid))
                    .isInstanceOf(SleeperPropertiesInvalidException.class);
        }

        @Test
        void shouldNotValidateWhenConstructingInstanceProperties() {
            // Given
            Properties properties = loadProperties(invalidInstanceProperties().saveAsString());

            // When / Then
            assertThatCode(() -> InstanceProperties.createWithoutValidation(properties))
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldNotValidateWhenConstructingTableProperties() {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            Properties properties = loadProperties(invalidTableProperties(instanceProperties).saveAsString());

            // When / Then
            assertThatCode(() -> new TableProperties(instanceProperties, properties))
                    .doesNotThrowAnyException();
        }
    }

    @DisplayName("Validate instance properties")
    @Nested
    class ValidateInstanceProperties {

        @Test
        void shouldFailValidationIfRequiredPropertyIsMissing() {
            // Given
            InstanceProperties instanceProperties = new InstanceProperties();
            instanceProperties.set(JARS_BUCKET, "jars");
            instanceProperties.set(VERSION, "0.1");
            instanceProperties.set(VPC_ID, "aVPC");
            instanceProperties.set(SUBNETS, "subnet1");

            // When / Then
            assertThatThrownBy(instanceProperties::validate)
                    .hasMessageContaining(ID.getPropertyName());
        }

        @Test
        void shouldFailValidationIfPropertyIsInvalid() {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            instanceProperties.set(MAXIMUM_CONNECTIONS_TO_S3, "-1");

            // When / Then
            assertThatThrownBy(instanceProperties::validate)
                    .hasMessageContaining(MAXIMUM_CONNECTIONS_TO_S3.getPropertyName());
        }

        @ParameterizedTest
        @ValueSource(strings = {"X86_64", "ARM64", "x86_64", "arm64"})
        void shouldPassValidationForSupportedEmrServerlessArchitecture(String architecture) {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE, architecture);

            // When / Then
            assertThatCode(instanceProperties::validate).doesNotThrowAnyException();
        }

        @ParameterizedTest
        @ValueSource(strings = {"X86_64,ARM64", "OTHER"})
        void shouldFailValidationForUnsupportedEmrServerlessArchitecture(String architecture) {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE, architecture);

            // When / Then
            assertThatThrownBy(instanceProperties::validate)
                    .hasMessageContaining(BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE.getPropertyName());
        }
    }

    @DisplayName("Validate table properties")
    @Nested
    class ValidateTableProperties {
        @Test
        void shouldFailValidationIfCompressionCodecIsInvalid() {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
            tableProperties.set(COMPRESSION_CODEC, "madeUp");
            // When / Then
            assertThatThrownBy(tableProperties::validate)
                    .hasMessageContaining("Property sleeper.table.parquet.compression.codec was invalid. It was \"madeUp\".");
        }

        @Test
        void shouldFailValidationIfTableNameIsAbsent() {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
            tableProperties.unset(TABLE_NAME);
            // When / Then
            assertThatThrownBy(tableProperties::validate)
                    .hasMessageContaining("Property sleeper.table.name was invalid. It was unset.");
        }

        @ParameterizedTest
        @ValueSource(strings = {"-1", "abc", "1.5"})
        void shouldFailValidationIfEmrServerlessDynamicAllocationExecutorCountIsInvalid(String value) {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
            tableProperties.set(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MIN_EXECUTORS, value);
            tableProperties.set(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS, value);
            tableProperties.set(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS, value);
            // When / Then
            assertThatThrownBy(tableProperties::validate)
                    .isInstanceOf(SleeperPropertiesInvalidException.class)
                    .extracting("invalidValues")
                    .isEqualTo(Map.of(
                            BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MIN_EXECUTORS, value,
                            BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS, value,
                            BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS, value));
        }

        @Test
        void shouldFailValidationIfEmrServerlessDynamicAllocationMaxExecutorsIsZero() {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
            tableProperties.set(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MIN_EXECUTORS, "0");
            tableProperties.set(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS, "0");
            // When / Then
            assertThatThrownBy(tableProperties::validate)
                    .isInstanceOf(SleeperPropertiesInvalidException.class)
                    .extracting("invalidValues")
                    .isEqualTo(Map.of(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS, "0"));
        }

        @Test
        void shouldFailValidationIfEmrServerlessDynamicAllocationInstanceDefaultIsInvalid() {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            instanceProperties.set(DEFAULT_BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS, "0");
            instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_OPTIMIZATION, "maybe");
            // When / Then
            assertThatThrownBy(instanceProperties::validate)
                    .isInstanceOf(SleeperPropertiesInvalidException.class)
                    .extracting("invalidValues")
                    .isEqualTo(Map.of(
                            DEFAULT_BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS, "0",
                            BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_OPTIMIZATION, "maybe"));
        }

        @ParameterizedTest
        @CsvSource({"0,3,29", "2,2,2", "0,0,1"})
        void shouldPassValidationIfEmrServerlessDynamicAllocationBoundsAreConsistent(String min, String initial, String max) {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
            tableProperties.set(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MIN_EXECUTORS, min);
            tableProperties.set(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS, initial);
            tableProperties.set(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS, max);
            // When / Then
            assertThatCode(tableProperties::validate).doesNotThrowAnyException();
        }

        @ParameterizedTest
        @CsvSource({"5,3,10", "0,11,10", "10,10,5"})
        void shouldFailValidationIfEmrServerlessDynamicAllocationBoundsAreInconsistent(String min, String initial, String max) {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
            tableProperties.set(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MIN_EXECUTORS, min);
            tableProperties.set(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS, initial);
            tableProperties.set(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS, max);
            // When / Then
            assertThatThrownBy(tableProperties::validate)
                    .isInstanceOf(SleeperPropertiesInvalidException.class)
                    .extracting("invalidValues")
                    .isEqualTo(Map.of(
                            BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MIN_EXECUTORS, min,
                            BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS, initial,
                            BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS, max));
        }

        @Test
        void shouldFailValidationIfTableEmrServerlessDynamicAllocationMaxIsBelowInheritedInitial() {
            // Given the instance default initial executors is 3
            InstanceProperties instanceProperties = createTestInstanceProperties();
            TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
            tableProperties.set(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS, "2");
            // When / Then
            assertThatThrownBy(tableProperties::validate)
                    .isInstanceOf(SleeperPropertiesInvalidException.class)
                    .extracting("invalidValues")
                    .isEqualTo(Map.of(
                            BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MIN_EXECUTORS, "0",
                            BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS, "3",
                            BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS, "2"));
        }

        @Test
        void shouldFailValidationIfEmrServerlessDynamicAllocationInstanceDefaultMaxIsBelowInitial() {
            // Given the instance default initial executors is 3
            InstanceProperties instanceProperties = createTestInstanceProperties();
            instanceProperties.set(DEFAULT_BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS, "2");
            // When / Then
            assertThatThrownBy(instanceProperties::validate)
                    .isInstanceOf(SleeperPropertiesInvalidException.class)
                    .extracting("invalidValues")
                    .isEqualTo(Map.of(
                            DEFAULT_BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MIN_EXECUTORS, "0",
                            DEFAULT_BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS, "3",
                            DEFAULT_BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_MAX_EXECUTORS, "2"));
        }
    }

    @Nested
    @DisplayName("Multiple validation errors")
    class MultipleValidationErrors {
        @Test
        void shouldFailValidationWithTwoInvalidInstanceProperties() {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            instanceProperties.set(BULK_IMPORT_JOB_FILE_RETENTION_DAYS, "abc");
            instanceProperties.set(MAXIMUM_CONNECTIONS_TO_S3, "def");

            // When
            assertThatThrownBy(instanceProperties::validate)
                    .isInstanceOf(SleeperPropertiesInvalidException.class)
                    .hasMessageContaining("Property sleeper.fs.s3a.max-connections was invalid. It was \"def\".")
                    .hasMessageContaining("Failure 1 of 2.")
                    .extracting("invalidValues")
                    .isEqualTo(Map.of(
                            BULK_IMPORT_JOB_FILE_RETENTION_DAYS, "abc",
                            MAXIMUM_CONNECTIONS_TO_S3, "def"));
        }

        @Test
        void shouldFailValidationWithTwoInvalidTableProperties() {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
            tableProperties.setNumber(COMPACTION_JOB_SEND_TIMEOUT_SECS, -100);
            tableProperties.set(COMPRESSION_CODEC, "madeUp");

            // When/Then
            assertThatThrownBy(tableProperties::validate)
                    .isInstanceOf(SleeperPropertiesInvalidException.class)
                    .hasMessageContaining("Property sleeper.table.parquet.compression.codec was invalid. It was \"madeUp\".")
                    .hasMessageContaining("Failure 1 of 2.")
                    .extracting("invalidValues")
                    .isEqualTo(Map.of(
                            COMPRESSION_CODEC, "madeUp",
                            COMPACTION_JOB_SEND_TIMEOUT_SECS, "-100"));
        }
    }
}
