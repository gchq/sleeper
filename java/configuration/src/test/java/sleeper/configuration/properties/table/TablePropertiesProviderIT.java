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

import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.junit.jupiter.api.Test;

import sleeper.configuration.table.index.DynamoDBTableIndex;

import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

class TablePropertiesProviderIT extends TablePropertiesITBase {

    private final TablePropertiesProvider provider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient);

    @Test
    void shouldLoadByName() {
        // Given
        store.save(tableProperties);

        // When / Then
        assertThat(provider.getByName(tableName)).isEqualTo(tableProperties);
    }

    @Test
    void shouldLoadByFullIdentifier() {
        // Given
        store.save(tableProperties);

        // When / Then
        assertThat(provider.get(tableProperties.getId())).isEqualTo(tableProperties);
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
        assertThat(provider.streamAllTableIds())
                .containsExactly(table1.getId(), table2.getId());
    }

    @Test
    void shouldReportTableDoesNotExistWhenNotInBucket() {
        // When / Then
        assertThat(provider.lookupByName(tableName))
                .isEmpty();
    }

    @Test
    void shouldThrowExceptionWhenTableDoesNotExist() {
        // When / Then
        assertThatThrownBy(() -> provider.getByName(tableName))
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void shouldReportTableExistsWhenInIndexButNotConfigBucket() {
        // Given
        new DynamoDBTableIndex(instanceProperties, dynamoDBClient)
                .create(tableProperties.getId());

        // When / Then
        assertThat(provider.lookupByName(tableName))
                .contains(tableProperties.getId());
    }

    @Test
    void shouldThrowExceptionWhenTableExistsInIndexButNotConfigBucket() {
        // Given
        new DynamoDBTableIndex(instanceProperties, dynamoDBClient)
                .create(tableProperties.getId());

        // When / Then
        assertThatThrownBy(() -> provider.getByName(tableName))
                .isInstanceOf(AmazonS3Exception.class);
    }
}
