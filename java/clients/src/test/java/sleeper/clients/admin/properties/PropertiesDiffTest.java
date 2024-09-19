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

package sleeper.clients.admin.properties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperProperty;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.configuration.properties.table.TableProperties;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.admin.properties.PropertiesDiffTestHelper.newValue;
import static sleeper.clients.admin.properties.PropertiesDiffTestHelper.valueChanged;
import static sleeper.clients.admin.properties.PropertiesDiffTestHelper.valueDeleted;
import static sleeper.clients.deploy.PopulatePropertiesTestHelper.generateTestInstanceProperties;
import static sleeper.clients.deploy.PopulatePropertiesTestHelper.generateTestTableProperties;
import static sleeper.configuration.properties.PropertiesUtils.loadProperties;
import static sleeper.configuration.properties.instance.CommonProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.configuration.properties.instance.CommonProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CONFIG;

public class PropertiesDiffTest {

    @DisplayName("Compare instance properties")
    @Nested
    class CompareInstanceProperties {

        @Test
        void shouldDetectNoChanges() {
            // Given
            InstanceProperties before = generateTestInstanceProperties();
            InstanceProperties after = generateTestInstanceProperties();

            // When / Then
            assertThat(getChanges(before, after)).isEmpty();
        }

        @Test
        void shouldDetectPropertyHasBeenUpdated() {
            // Given
            InstanceProperties before = generateTestInstanceProperties();
            before.set(MAXIMUM_CONNECTIONS_TO_S3, "30");
            InstanceProperties after = generateTestInstanceProperties();
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "50");

            // When / Then
            assertThat(getChanges(before, after))
                    .containsExactly(valueChanged(MAXIMUM_CONNECTIONS_TO_S3, "30", "50"));
        }

        @Test
        void shouldDetectPropertyIsNewlySet() {
            // Given
            InstanceProperties before = generateTestInstanceProperties();
            InstanceProperties after = generateTestInstanceProperties();
            after.set(INGEST_SOURCE_BUCKET, "some-bucket");

            // When / Then
            assertThat(getChanges(before, after))
                    .containsExactly(newValue(INGEST_SOURCE_BUCKET, "some-bucket"));
        }

        @Test
        void shouldDetectPropertyIsUnset() {
            // Given
            InstanceProperties before = generateTestInstanceProperties();
            before.set(INGEST_SOURCE_BUCKET, "some-bucket");
            InstanceProperties after = generateTestInstanceProperties();

            // When / Then
            assertThat(getChanges(before, after))
                    .containsExactly(valueDeleted(INGEST_SOURCE_BUCKET, "some-bucket"));
        }

        @Test
        void shouldDetectDefaultedPropertyIsNewlySet() {
            // Given
            InstanceProperties before = generateTestInstanceProperties();
            InstanceProperties after = generateTestInstanceProperties();
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "50");

            // When / Then
            assertThat(getChanges(before, after))
                    .containsExactly(newValue(MAXIMUM_CONNECTIONS_TO_S3, "50"));
        }

        @Test
        void shouldDetectDefaultedPropertyIsUnset() {
            // Given
            InstanceProperties before = generateTestInstanceProperties();
            before.set(MAXIMUM_CONNECTIONS_TO_S3, "50");
            InstanceProperties after = generateTestInstanceProperties();

            // When / Then
            assertThat(getChanges(before, after))
                    .containsExactly(valueDeleted(MAXIMUM_CONNECTIONS_TO_S3, "50"));
        }
    }

    @DisplayName("Compare unknown properties")
    @Nested
    class CompareUnknownProperties {
        @Test
        void shouldDetectNoChanges() {
            // Given
            InstanceProperties before = new InstanceProperties(
                    loadProperties("unknown.property=1"));
            InstanceProperties after = new InstanceProperties(
                    loadProperties("unknown.property=1"));

            // When / Then
            assertThat(getChanges(before, after)).isEmpty();
        }

        @Test
        void shouldDetectPropertyHasBeenUpdated() {
            // Given
            InstanceProperties before = new InstanceProperties(
                    loadProperties("unknown.property=1"));
            InstanceProperties after = new InstanceProperties(
                    loadProperties("unknown.property=2"));

            // When / Then
            assertThat(getChanges(before, after))
                    .containsExactly(valueChanged("unknown.property", "1", "2"));
        }

        @Test
        void shouldDetectPropertyIsNewlySet() {
            // Given
            InstanceProperties before = new InstanceProperties();
            InstanceProperties after = new InstanceProperties(
                    loadProperties("unknown.property=12"));

            // When / Then
            assertThat(getChanges(before, after))
                    .containsExactly(newValue("unknown.property", "12"));
        }

        @Test
        void shouldDetectPropertyIsUnset() {
            // Given
            InstanceProperties before = new InstanceProperties(
                    loadProperties("unknown.property=12"));
            InstanceProperties after = new InstanceProperties();

            // When / Then
            assertThat(getChanges(before, after))
                    .containsExactly(valueDeleted("unknown.property", "12"));
        }
    }

    @DisplayName("Compare table properties")
    @Nested
    class CompareTableProperties {

        @Test
        void shouldDetectPropertyHasBeenUpdated() {
            // Given
            TableProperties before = generateTestTableProperties();
            before.set(ITERATOR_CONFIG, "config-before");
            TableProperties after = generateTestTableProperties();
            after.set(ITERATOR_CONFIG, "config-after");

            // When / Then
            assertThat(getChanges(before, after))
                    .containsExactly(valueChanged(ITERATOR_CONFIG, "config-before", "config-after"));
        }
    }

    @DisplayName("Combine multiple diffs")
    @Nested
    class CombineDiffs {
        @Test
        void shouldCombineTwoDiffsIntoOne() {
            // Given
            PropertiesDiff diff1 = generateSingleDiff(MAXIMUM_CONNECTIONS_TO_S3, "123", "456");
            PropertiesDiff diff2 = generateSingleDiff(MAXIMUM_CONNECTIONS_TO_S3, "456", "789");

            assertThat(diff1.andThen(diff2).getChanges())
                    .containsExactly(valueChanged(MAXIMUM_CONNECTIONS_TO_S3, "123", "789"));
        }

        @Test
        void shouldCancelOutDiffsWhenChangesHaveBeenReverted() {
            // Given
            PropertiesDiff diff1 = generateSingleDiff(MAXIMUM_CONNECTIONS_TO_S3, "123", "456");
            PropertiesDiff diff2 = generateSingleDiff(MAXIMUM_CONNECTIONS_TO_S3, "456", "123");

            assertThat(diff1.andThen(diff2).getChanges())
                    .isEmpty();
        }

        @Test
        void shouldCancelOutDiffsWhenChangesHaveBeenRevertedAfterMultipleChanges() {
            // Given
            PropertiesDiff diff1 = generateSingleDiff(MAXIMUM_CONNECTIONS_TO_S3, "123", "456");
            PropertiesDiff diff2 = generateSingleDiff(MAXIMUM_CONNECTIONS_TO_S3, "456", "789");
            PropertiesDiff diff3 = generateSingleDiff(MAXIMUM_CONNECTIONS_TO_S3, "789", "123");

            assertThat(diff1.andThen(diff2).andThen(diff3).getChanges())
                    .isEmpty();
        }

        @Test
        void shouldNotCombineDiffsWhenPropertyIsDifferent() {
            // Given
            PropertiesDiff diff1 = generateSingleDiff(MAXIMUM_CONNECTIONS_TO_S3, "123", "456");
            PropertiesDiff diff2 = generateSingleDiff(LOG_RETENTION_IN_DAYS, "456", "123");

            assertThat(diff1.andThen(diff2).getChanges())
                    .containsExactly(
                            valueChanged(MAXIMUM_CONNECTIONS_TO_S3, "123", "456"),
                            valueChanged(LOG_RETENTION_IN_DAYS, "456", "123"));
        }

        private PropertiesDiff generateSingleDiff(InstanceProperty property, String oldValue, String newValue) {
            InstanceProperties before = generateTestInstanceProperties();
            before.set(property, oldValue);
            InstanceProperties after = generateTestInstanceProperties();
            after.set(property, newValue);
            return new PropertiesDiff(before, after);
        }
    }

    private <T extends SleeperProperty> List<PropertyDiff> getChanges(SleeperProperties<T> before, SleeperProperties<T> after) {
        return new PropertiesDiff(before, after).getChanges();
    }
}
