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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CommonProperty.TABLE_PROPERTIES_PROVIDER_TIMEOUT_IN_MINS;
import static sleeper.configuration.properties.table.TableProperty.ROW_GROUP_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

class TablePropertiesProviderIT extends TablePropertiesITBase {

    private final TablePropertiesProvider provider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient);

    private TablePropertiesProvider providerWithTimes(Instant... times) {
        return new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient,
                List.of(times).iterator()::next);
    }

    @Nested
    @DisplayName("Load table properties")
    class LoadProperties {

        @Test
        void shouldLoadByName() {
            // Given
            store.save(tableProperties);

            // When / Then
            assertThat(provider.getTableProperties(tableName)).isEqualTo(tableProperties);
            assertThat(provider.getTablePropertiesIfExists(tableName)).contains(tableProperties);
        }

        @Test
        void shouldLoadByFullIdentifier() {
            // Given
            store.save(tableProperties);

            // When / Then
            assertThat(provider.getTableProperties(tableProperties.getId())).isEqualTo(tableProperties);
        }

        @Test
        void shouldReportTableDoesNotExistWhenNotInBucket() {
            // When / Then
            assertThat(provider.getTablePropertiesIfExists(tableName))
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("Load all tables")
    class LoadAllTables {

        @Test
        void shouldLoadAllTables() {
            // Given
            TableProperties table1 = createValidTableProperties();
            TableProperties table2 = createValidTableProperties();
            store.save(table1);
            store.save(table2);

            // When / Then
            assertThat(provider.streamAllTables())
                    .containsExactlyInAnyOrder(table1, table2);
            assertThat(provider.streamAllTableIds())
                    .containsExactlyInAnyOrder(table1.getId(), table2.getId());
        }

        @Test
        void shouldCachePropertiesAfterLoadingAllTables() {
            // Given
            tableProperties.setNumber(ROW_GROUP_SIZE, 123);
            store.save(tableProperties);

            provider.streamAllTables().forEach(properties -> {
            });

            tableProperties.setNumber(ROW_GROUP_SIZE, 456);
            store.save(tableProperties);

            // When / Then
            assertThat(provider.getTableProperties(tableName).getInt(ROW_GROUP_SIZE))
                    .isEqualTo(123);
        }

        @Test
        void shouldRetrieveFromCacheWhenLoadingAllTables() {
            // Given
            tableProperties.setNumber(ROW_GROUP_SIZE, 123);
            store.save(tableProperties);

            provider.getTableProperties(tableName);

            tableProperties.setNumber(ROW_GROUP_SIZE, 456);
            store.save(tableProperties);

            // When / Then
            assertThat(provider.streamAllTables()
                    .map(properties -> properties.getInt(ROW_GROUP_SIZE)))
                    .contains(123);
        }
    }

    @Nested
    @DisplayName("Expire cached properties on a timeout")
    class ExpireCacheOnTimeout {

        @Test
        void shouldReloadPropertiesFromS3WhenTimeoutReachedForTable() {
            // Given
            tableProperties.setNumber(ROW_GROUP_SIZE, 123L);
            store.save(tableProperties);
            instanceProperties.setNumber(TABLE_PROPERTIES_PROVIDER_TIMEOUT_IN_MINS, 3);
            TablePropertiesProvider provider = providerWithTimes(
                    Instant.parse("2023-10-09T17:11:00Z"),
                    Instant.parse("2023-10-09T17:15:00Z"));

            // When
            provider.getTableProperties(tableName); // Populate cache
            tableProperties.setNumber(ROW_GROUP_SIZE, 456L);
            store.save(tableProperties);

            // Then
            assertThat(provider.getTableProperties(tableName).getLong(ROW_GROUP_SIZE))
                    .isEqualTo(456L);
        }

        @Test
        void shouldNotReloadPropertiesFromS3WhenTimeoutHasNotBeenReachedForTable() {
            // Given
            tableProperties.setNumber(ROW_GROUP_SIZE, 123L);
            store.save(tableProperties);
            instanceProperties.setNumber(TABLE_PROPERTIES_PROVIDER_TIMEOUT_IN_MINS, 3);
            TablePropertiesProvider provider = providerWithTimes(
                    Instant.parse("2023-10-09T17:11:00Z"),
                    Instant.parse("2023-10-09T17:12:00Z"));

            // When
            provider.getTableProperties(tableName); // Populate cache
            tableProperties.setNumber(ROW_GROUP_SIZE, 456L);
            store.save(tableProperties);

            // Then
            assertThat(provider.getTableProperties(tableName).getLong(ROW_GROUP_SIZE))
                    .isEqualTo(123L);
        }

        @Test
        void shouldNotReloadPropertiesFromS3WhenTimeoutHasBeenReachedForOtherTable() {
            // Given
            TableProperties tableProperties1 = createValidTableProperties();
            TableProperties tableProperties2 = createValidTableProperties();
            tableProperties1.setNumber(ROW_GROUP_SIZE, 123L);
            tableProperties2.setNumber(ROW_GROUP_SIZE, 123L);
            store.save(tableProperties1);
            store.save(tableProperties2);
            instanceProperties.setNumber(TABLE_PROPERTIES_PROVIDER_TIMEOUT_IN_MINS, 3);
            TablePropertiesProvider provider = providerWithTimes(
                    Instant.parse("2023-10-09T17:11:00Z"),
                    Instant.parse("2023-10-09T17:14:00Z"),
                    Instant.parse("2023-10-09T17:15:00Z"),
                    Instant.parse("2023-10-09T17:15:00Z"));

            // When
            provider.getTableProperties(tableProperties1.get(TABLE_NAME)); // Populate cache
            provider.getTableProperties(tableProperties2.get(TABLE_NAME)); // Populate cache
            tableProperties1.setNumber(ROW_GROUP_SIZE, 456L);
            tableProperties2.setNumber(ROW_GROUP_SIZE, 456L);
            store.save(tableProperties1);
            store.save(tableProperties2);

            // Then
            assertThat(provider.getTableProperties(tableProperties1.get(TABLE_NAME)).getLong(ROW_GROUP_SIZE))
                    .isEqualTo(456L);
            assertThat(provider.getTableProperties(tableProperties2.get(TABLE_NAME)).getLong(ROW_GROUP_SIZE))
                    .isEqualTo(123L);
        }
    }
}
