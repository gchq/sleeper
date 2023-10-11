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

import sleeper.configuration.properties.instance.InstanceProperties;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CommonProperty.TABLE_PROPERTIES_PROVIDER_TIMEOUT_IN_MINS;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.ROW_GROUP_SIZE;

class TablePropertiesProviderIT extends TablePropertiesS3TestBase {

    @Test
    void shouldLoadFromS3() {
        // Given
        TableProperties validProperties = createValidPropertiesWithTableNameAndBucket(
                "test", "provider-load");
        s3Client.createBucket("provider-load");
        validProperties.saveToS3(s3Client);

        // When
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CONFIG_BUCKET, "provider-load");
        TablePropertiesProvider provider = new TablePropertiesProvider(s3Client, instanceProperties);

        // Then
        assertThat(provider.getTableProperties("test")).isEqualTo(validProperties);
        assertThat(provider.getTablePropertiesIfExists("test")).contains(validProperties);
    }

    @Test
    void shouldReportTableDoesNotExistWhenNotInBucket() {
        // Given
        s3Client.createBucket("provider-no-table");

        // When
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CONFIG_BUCKET, "provider-no-table");
        TablePropertiesProvider provider = new TablePropertiesProvider(s3Client, instanceProperties);

        // Then
        assertThat(provider.getTablePropertiesIfExists("test"))
                .isEmpty();
    }

    @Test
    void shouldReloadPropertiesFromS3WhenTimeoutReachedForTable() {
        // Given
        TableProperties validProperties = createValidPropertiesWithTableNameAndBucket(
                "test", "provider-load");
        s3Client.createBucket("provider-load");
        validProperties.setNumber(ROW_GROUP_SIZE, 123L);
        validProperties.saveToS3(s3Client);
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CONFIG_BUCKET, "provider-load");
        instanceProperties.setNumber(TABLE_PROPERTIES_PROVIDER_TIMEOUT_IN_MINS, 3);
        TablePropertiesProvider provider = new TablePropertiesProvider(s3Client, instanceProperties,
                List.of(
                        Instant.parse("2023-10-09T17:11:00Z"),
                        Instant.parse("2023-10-09T17:15:00Z")
                ).iterator()::next);

        // When
        provider.getTableProperties("test"); // Populate cache
        validProperties.setNumber(ROW_GROUP_SIZE, 456L);
        validProperties.saveToS3(s3Client);

        // Then
        assertThat(provider.getTableProperties("test").getLong(ROW_GROUP_SIZE))
                .isEqualTo(456L);
    }

    @Test
    void shouldNotReloadPropertiesFromS3WhenTimeoutHasNotBeenReachedForTable() {
        // Given
        TableProperties validProperties = createValidPropertiesWithTableNameAndBucket(
                "test", "provider-load");
        s3Client.createBucket("provider-load");
        validProperties.setNumber(ROW_GROUP_SIZE, 123L);
        validProperties.saveToS3(s3Client);
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CONFIG_BUCKET, "provider-load");
        instanceProperties.setNumber(TABLE_PROPERTIES_PROVIDER_TIMEOUT_IN_MINS, 3);
        TablePropertiesProvider provider = new TablePropertiesProvider(s3Client, instanceProperties,
                List.of(
                        Instant.parse("2023-10-09T17:11:00Z"),
                        Instant.parse("2023-10-09T17:12:00Z")
                ).iterator()::next);

        // When
        provider.getTableProperties("test"); // Populate cache
        validProperties.setNumber(ROW_GROUP_SIZE, 456L);
        validProperties.saveToS3(s3Client);

        // Then
        assertThat(provider.getTableProperties("test").getLong(ROW_GROUP_SIZE))
                .isEqualTo(123L);
    }

    @Test
    void shouldNotReloadPropertiesFromS3WhenTimeoutHasBeenReachedForOtherTable() {
        // Given
        TableProperties validProperties1 = createValidPropertiesWithTableNameAndBucket(
                "table-1", "provider-load");
        TableProperties validProperties2 = createValidPropertiesWithTableNameAndBucket(
                "table-2", "provider-load");
        s3Client.createBucket("provider-load");
        validProperties1.setNumber(ROW_GROUP_SIZE, 123L);
        validProperties1.saveToS3(s3Client);
        validProperties2.setNumber(ROW_GROUP_SIZE, 123L);
        validProperties2.saveToS3(s3Client);
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CONFIG_BUCKET, "provider-load");
        instanceProperties.setNumber(TABLE_PROPERTIES_PROVIDER_TIMEOUT_IN_MINS, 3);
        TablePropertiesProvider provider = new TablePropertiesProvider(s3Client, instanceProperties,
                List.of(
                        Instant.parse("2023-10-09T17:11:00Z"),
                        Instant.parse("2023-10-09T17:14:00Z"),
                        Instant.parse("2023-10-09T17:15:00Z"),
                        Instant.parse("2023-10-09T17:15:00Z")
                ).iterator()::next);

        // When
        provider.getTableProperties("table-1"); // Populate cache
        provider.getTableProperties("table-2"); // Populate cache
        validProperties1.setNumber(ROW_GROUP_SIZE, 456L);
        validProperties1.saveToS3(s3Client);
        validProperties2.setNumber(ROW_GROUP_SIZE, 456L);
        validProperties2.saveToS3(s3Client);

        // Then
        assertThat(provider.getTableProperties("table-1").getLong(ROW_GROUP_SIZE))
                .isEqualTo(456L);
        assertThat(provider.getTableProperties("table-2").getLong(ROW_GROUP_SIZE))
                .isEqualTo(123L);
    }
}
