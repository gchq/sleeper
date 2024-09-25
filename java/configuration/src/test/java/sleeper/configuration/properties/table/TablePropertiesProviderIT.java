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

package sleeper.configuration.properties.table;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.table.TableNotFoundException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

class TablePropertiesProviderIT extends TablePropertiesITBase {

    private final TablePropertiesProvider provider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDBClient);

    @Test
    void shouldLoadByName() {
        // Given
        store.save(tableProperties);

        // When / Then
        assertThat(provider.getByName(tableName))
                .isEqualTo(tableProperties);
    }

    @Test
    void shouldLoadById() {
        // Given
        store.save(tableProperties);

        // When / Then
        assertThat(provider.getById(tableId))
                .isEqualTo(tableProperties);
    }

    @Test
    void shouldLoadAllTables() {
        // Given
        TableProperties table1 = createValidTableProperties();
        TableProperties table2 = createValidTableProperties();
        table1.set(TABLE_NAME, "table-1");
        table2.set(TABLE_NAME, "table-2");
        store.save(table1);
        store.save(table2);

        // When / Then
        assertThat(provider.streamAllTables())
                .containsExactly(table1, table2);
    }

    @Test
    void shouldThrowExceptionWhenTableDoesNotExist() {
        // When / Then
        assertThatThrownBy(() -> provider.getByName(tableName))
                .isInstanceOf(TableNotFoundException.class);
    }

    @Test
    void shouldThrowExceptionWhenTableExistsInIndexButNotConfigBucket() {
        // Given
        new DynamoDBTableIndex(instanceProperties, dynamoDBClient)
                .create(tableProperties.getStatus());

        // When / Then
        assertThatThrownBy(() -> provider.getByName(tableName))
                .isInstanceOf(TableNotFoundException.class);
    }

    @Test
    void shouldNotLoadByIdWhenNotInIndex() {
        // Given
        store.save(tableProperties);
        new DynamoDBTableIndex(instanceProperties, dynamoDBClient)
                .delete(tableProperties.getStatus());

        // When / Then
        assertThatThrownBy(() -> provider.getById(tableId))
                .isInstanceOf(TableNotFoundException.class);
    }
}
