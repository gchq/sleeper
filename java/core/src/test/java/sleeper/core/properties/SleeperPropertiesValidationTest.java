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

package sleeper.core.properties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.PropertiesUtils.loadProperties;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.core.properties.instance.CommonProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.core.properties.instance.CommonProperty.REGION;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.core.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

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
            TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
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
            // Given - no account set
            InstanceProperties instanceProperties = new InstanceProperties();
            instanceProperties.set(REGION, "eu-west-2");
            instanceProperties.set(JARS_BUCKET, "jars");
            instanceProperties.set(VERSION, "0.1");
            instanceProperties.set(ID, "test");
            instanceProperties.set(VPC_ID, "aVPC");
            instanceProperties.set(SUBNETS, "subnet1");

            // When / Then
            assertThatThrownBy(instanceProperties::validate)
                    .hasMessageContaining(ACCOUNT.getPropertyName());
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
    }

    @DisplayName("Validate table properties")
    @Nested
    class ValidateTableProperties {
        @Test
        void shouldFailValidationIfCompressionCodecIsInvalid() {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
            tableProperties.set(COMPRESSION_CODEC, "madeUp");
            // When / Then
            assertThatThrownBy(tableProperties::validate)
                    .hasMessage("Property sleeper.table.compression.codec was invalid. It was \"madeUp\".");
        }

        @Test
        void shouldFailValidationIfTableNameIsAbsent() {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
            tableProperties.unset(TABLE_NAME);
            // When / Then
            assertThatThrownBy(tableProperties::validate)
                    .hasMessage("Property sleeper.table.name was invalid. It was unset.");
        }

        @Test
        void shouldFailValidationIfCompactionFilesBatchSizeTooLargeForDynamoDBStateStore() {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
            tableProperties.set(STATESTORE_CLASSNAME, "sleeper.statestore.dynamodb.DynamoDBStateStore");
            tableProperties.setNumber(COMPACTION_FILES_BATCH_SIZE, 50);

            // When/Then
            assertThatThrownBy(tableProperties::validate)
                    .isInstanceOf(SleeperPropertiesInvalidException.class)
                    .hasMessage("Property sleeper.table.compaction.files.batch.size was invalid. " +
                            "It was \"50\".");
        }

        @Test
        void shouldPassValidationIfCompactionFilesBatchSizeTooLargeForDynamoDBStateStoreButS3StateStoreChosen() {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
            tableProperties.set(STATESTORE_CLASSNAME, "sleeper.statestore.s3.S3StateStore");
            tableProperties.setNumber(COMPACTION_FILES_BATCH_SIZE, 50);

            // When/Then
            assertThatCode(tableProperties::validate).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("Multiple validation errors")
    class MultipleValidationErrors {
        @Test
        void shouldFailValidationWithTwoInvalidInstanceProperties() {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            instanceProperties.set(LOG_RETENTION_IN_DAYS, "abc");
            instanceProperties.set(MAXIMUM_CONNECTIONS_TO_S3, "def");

            // When
            assertThatThrownBy(instanceProperties::validate)
                    .isInstanceOf(SleeperPropertiesInvalidException.class)
                    .hasMessage("Property sleeper.log.retention.days was invalid. It was \"abc\". Failure 1 of 2.")
                    .extracting("invalidValues")
                    .isEqualTo(Map.of(
                            LOG_RETENTION_IN_DAYS, "abc",
                            MAXIMUM_CONNECTIONS_TO_S3, "def"));
        }

        @Test
        void shouldFailValidationForCustomValidationAndPropertyWithValidationPredicate() {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
            tableProperties.set(STATESTORE_CLASSNAME, "sleeper.statestore.dynamodb.DynamoDBStateStore");
            tableProperties.setNumber(COMPACTION_FILES_BATCH_SIZE, 50);
            tableProperties.set(COMPRESSION_CODEC, "madeUp");

            // When/Then
            assertThatThrownBy(tableProperties::validate)
                    .isInstanceOf(SleeperPropertiesInvalidException.class)
                    .hasMessage("Property sleeper.table.compression.codec was invalid. It was \"madeUp\". Failure 1 of 2.")
                    .extracting("invalidValues")
                    .isEqualTo(Map.of(
                            COMPRESSION_CODEC, "madeUp",
                            COMPACTION_FILES_BATCH_SIZE, "50"));
        }
    }
}
