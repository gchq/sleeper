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

package sleeper.configuration.table.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableAlreadyExistsException;
import sleeper.core.table.TableIdGenerator;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.table.TableStatus;
import sleeper.core.table.TableStatusTestHelper;
import sleeper.dynamodb.test.DynamoDBTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;

public class DynamoDBTableIndexIT extends DynamoDBTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final DynamoDBTableIndex index = new DynamoDBTableIndex(instanceProperties, dynamoDBClient);
    private final TableIdGenerator idGenerator = new TableIdGenerator();

    @BeforeEach
    void setUp() {
        DynamoDBTableIndexCreator.create(dynamoDBClient, instanceProperties);
    }

    @Nested
    @DisplayName("Create a table")
    class CreateTable {
        @Test
        void shouldCreateATable() {
            TableStatus table = createTable("test-table");

            assertThat(index.streamAllTables())
                    .containsExactly(table);
        }

        @Test
        void shouldPutTableOnlineWhenItIsCreated() {
            TableStatus tableId = createTable("test-table");

            assertThat(index.streamOnlineTables())
                    .containsExactly(tableId);
        }

        @Test
        void shouldFailToCreateATableWhichAlreadyExists() {
            createTable("duplicate-table");

            assertThatThrownBy(() -> createTable("duplicate-table"))
                    .isInstanceOf(TableAlreadyExistsException.class);
        }
    }

    @Nested
    @DisplayName("Look up a table")
    class LookupTable {

        @Test
        void shouldGetTableByName() {
            TableStatus table = createTable("test-table");

            assertThat(index.getTableByName("test-table"))
                    .contains(table);
        }

        @Test
        void shouldGetNoTableByName() {
            createTable("existing-table");

            assertThat(index.getTableByName("not-a-table"))
                    .isEmpty();
        }

        @Test
        void shouldGetTableById() {
            TableStatus table = createTable("test-table");

            assertThat(index.getTableByUniqueId(table.getTableUniqueId()))
                    .contains(table);
        }

        @Test
        void shouldGetNoTableById() {
            createTable("existing-table");

            assertThat(index.getTableByUniqueId("not-a-table"))
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("List tables")
    class ListTables {

        @Test
        void shouldGetTablesOrderedByName() {
            createTable("some-table");
            createTable("a-table");
            createTable("this-table");
            createTable("other-table");

            assertThat(index.streamAllTables())
                    .extracting(TableStatus::getTableName)
                    .containsExactly(
                            "a-table",
                            "other-table",
                            "some-table",
                            "this-table");
        }

        @Test
        void shouldGetTableIds() {
            TableStatus table1 = createTable("first-table");
            TableStatus table2 = createTable("second-table");

            assertThat(index.streamAllTables())
                    .containsExactly(table1, table2);
        }

        @Test
        void shouldGetNoTables() {
            assertThat(index.streamAllTables()).isEmpty();
        }

        @Test
        void shouldGetOnlineTables() {
            // Given
            TableStatus table1 = createTable("online-table");
            TableStatus table2 = createTable("offline-table");
            index.update(table2.takeOffline());

            // When / Then
            assertThat(index.streamOnlineTables())
                    .containsExactly(table1);
        }
    }

    @Nested
    @DisplayName("Delete table")
    class DeleteTable {

        @Test
        void shouldDeleteTableNameReference() {
            // Given
            TableStatus table = createTable("test-table");

            // When
            index.delete(table);

            // Then
            assertThat(index.getTableByName("test-table")).isEmpty();
        }

        @Test
        void shouldDeleteTableIdReference() {
            // Given
            TableStatus table = createTable("test-table");

            // When
            index.delete(table);

            // Then
            assertThat(index.getTableByUniqueId(table.getTableUniqueId())).isEmpty();
        }

        @Test
        void shouldDeleteAllTablesWhileStreamingThroughIds() {
            // Given
            createTable("test-table-1");
            createTable("test-table-2");

            // When
            index.streamAllTables().forEach(index::delete);

            // Then
            assertThat(index.streamAllTables()).isEmpty();
        }

        @Test
        void shouldFailToDeleteTableWhenTableNameHasBeenUpdated() {
            // Given
            TableStatus oldTable = createTable("old-name");
            TableStatus newTable = TableStatusTestHelper.uniqueIdAndName(oldTable.getTableUniqueId(), "new-name");
            index.update(newTable);

            // When / Then
            assertThatThrownBy(() -> index.delete(oldTable))
                    .isInstanceOf(TableNotFoundException.class);
            assertThat(index.streamAllTables()).contains(newTable);
            assertThat(index.getTableByName("old-name")).isEmpty();
            assertThat(index.getTableByName("new-name")).contains(newTable);
        }

        @Test
        void shouldFailToDeleteTableThatDoesNotExist() {
            // Given
            TableStatus table = TableStatusTestHelper.uniqueIdAndName("not-a-table-id", "not-a-table");

            // When / Then
            assertThatThrownBy(() -> index.delete(table))
                    .isInstanceOf(TableNotFoundException.class);
        }

        @Test
        void shouldFailToDeleteTableIfTableRenamedAfterLoadingOldId() {
            // Given
            TableStatus old = TableStatusTestHelper.uniqueIdAndName("test-id", "old-name");
            TableStatus renamed = TableStatusTestHelper.uniqueIdAndName("test-id", "changed-name");
            index.create(old);
            index.update(renamed);

            // When/Then
            assertThatThrownBy(() -> index.delete(old))
                    .isInstanceOf(TableNotFoundException.class);
            assertThat(index.streamAllTables()).contains(renamed);
        }

        @Test
        void shouldFailToDeleteTableIfTableDeletedAndRecreatedAfterLoadingOldId() {
            // Given
            TableStatus old = TableStatusTestHelper.uniqueIdAndName("test-id-1", "table-name");
            TableStatus recreated = TableStatusTestHelper.uniqueIdAndName("test-id-2", "table-name");
            index.create(recreated);

            // When/Then
            assertThatThrownBy(() -> index.delete(old))
                    .isInstanceOf(TableNotFoundException.class);
        }
    }

    @Nested
    @DisplayName("Update table")
    class UpdateTable {
        @Test
        void shouldUpdateTableName() {
            // Given
            TableStatus table = createTable("old-name");

            // When
            TableStatus newTable = TableStatusTestHelper.uniqueIdAndName(table.getTableUniqueId(), "new-name");
            index.update(newTable);

            // Then
            assertThat(index.streamAllTables())
                    .containsExactly(newTable);
            assertThat(index.getTableByName("new-name"))
                    .contains(newTable);
            assertThat(index.getTableByName("old-name")).isEmpty();
            assertThat(index.getTableByUniqueId(newTable.getTableUniqueId()))
                    .contains(newTable);
        }

        @Test
        void shouldFailToUpdateTableIfTableDoesNotExist() {
            // Given
            TableStatus newTable = TableStatusTestHelper.uniqueIdAndName("not-a-table-id", "new-name");

            // When/Then
            assertThatThrownBy(() -> index.update(newTable))
                    .isInstanceOf(TableNotFoundException.class);
            assertThat(index.streamAllTables()).isEmpty();
        }

        @Test
        void shouldFailToUpdateTableIfTableWithSameNameAlreadyExists() {
            // Given
            createTable("test-name-1");
            TableStatus table2 = createTable("test-name-2");

            // When / Then
            TableStatus newTable = TableStatusTestHelper.uniqueIdAndName(table2.getTableUniqueId(), "test-name-1");
            assertThatThrownBy(() -> index.update(newTable))
                    .isInstanceOf(TableAlreadyExistsException.class);
        }

        @Test
        void shouldNotThrowExceptionWhenUpdatingTableWithNoChanges() {
            // Given
            TableStatus table = createTable("test-name-1");

            // When / Then
            assertThatCode(() -> index.update(table))
                    .doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("Take offline")
    class TakeOffline {
        @Test
        void shouldTakeTableOffline() {
            // Given
            TableStatus table = createTable("test-table");

            // When
            TableStatus offlineTable = table.takeOffline();
            index.update(offlineTable);

            // Then
            assertThat(index.streamAllTables()).containsExactly(offlineTable);
            assertThat(index.streamOnlineTables()).isEmpty();
            assertThat(index.getTableByUniqueId(table.getTableUniqueId()).stream())
                    .containsExactly(offlineTable);
            assertThat(index.getTableByName(table.getTableName()).stream())
                    .containsExactly(offlineTable);
        }

        @Test
        void shouldFailToTakeTableOfflineIfTableDoesNotExist() {
            // When / Then
            assertThatThrownBy(() -> index.update(TableStatusTestHelper.uniqueIdAndName("not-a-table-id", "not-a-table").takeOffline()))
                    .isInstanceOf(TableNotFoundException.class);
            assertThat(index.streamAllTables()).isEmpty();
        }

        @Test
        void shouldFailToTakeTableOfflineIfTableHasBeenDeleted() {
            // Given
            TableStatus table = createTable("test-table");
            index.delete(table);

            // When / Then
            assertThatThrownBy(() -> index.update(table.takeOffline()))
                    .isInstanceOf(TableNotFoundException.class);
            assertThat(index.streamAllTables()).isEmpty();
            assertThat(index.streamOnlineTables()).isEmpty();
            assertThat(index.getTableByUniqueId(table.getTableUniqueId())).isEmpty();
            assertThat(index.getTableByName(table.getTableName())).isEmpty();
        }
    }

    @Nested
    @DisplayName("Put table online")
    class PutOnline {
        @Test
        void shouldPutTableOnline() {
            // Given
            TableStatus table = createTable("test-table");
            TableStatus onlineTable = table.putOnline();
            TableStatus offlineTable = table.takeOffline();
            index.update(offlineTable);

            // When
            index.update(onlineTable);

            // Then
            assertThat(index.streamAllTables()).containsExactly(onlineTable);
            assertThat(index.streamOnlineTables()).containsExactly(onlineTable);
            assertThat(index.getTableByUniqueId(table.getTableUniqueId()).stream())
                    .containsExactly(onlineTable);
            assertThat(index.getTableByName(table.getTableName()).stream())
                    .containsExactly(onlineTable);
        }

        @Test
        void shouldFailToPutTableOnlineWhenTableDoesNotExist() {
            // When / Then
            assertThatThrownBy(() -> index.update(TableStatusTestHelper.uniqueIdAndName("not-a-table-id", "not-a-table").putOnline()))
                    .isInstanceOf(TableNotFoundException.class);
            assertThat(index.streamAllTables()).isEmpty();
        }
    }

    private TableStatus createTable(String tableName) {
        TableStatus table = TableStatusTestHelper.uniqueIdAndName(idGenerator.generateString(), tableName);
        index.create(table);
        return table;
    }
}
