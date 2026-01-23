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
package sleeper.core.statestore;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.util.JvmMemoryUse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_PROVIDER_CACHE_SIZE;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_PROVIDER_MIN_FREE_HEAP_TARGET_AMOUNT;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_PROVIDER_MIN_FREE_HEAP_TARGET_PERCENTAGE;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class StateStoreProviderTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = createSchemaWithKey("key");
    private final Map<String, StateStore> tableIdToStateStore = new HashMap<>();
    private final List<String> tablesLoaded = new ArrayList<>();
    private JvmMemoryUse.Provider memoryProvider = () -> new JvmMemoryUse(0, 0, Long.MAX_VALUE);

    @Test
    void shouldCacheStateStore() {
        // Given
        TableProperties table = createTable("test-table-id", "test-table");
        StateStore store = createStateStore(table);

        // When
        StateStoreProvider provider = provider();
        StateStore retrievedStore1 = provider.getStateStore(table);
        StateStore retrievedStore2 = provider.getStateStore(table);
        StateStore retrievedStore3 = provider.getStateStore(table);

        // Then
        assertThat(retrievedStore1).isSameAs(store);
        assertThat(retrievedStore2).isSameAs(store);
        assertThat(retrievedStore3).isSameAs(store);
        assertThat(tablesLoaded).containsExactly("test-table-id");
    }

    @Nested
    @DisplayName("Maximum number of tables in cache")
    class MaxTables {
        // TODO remove least recently used from cache instead of least recently added

        @Test
        void shouldRemoveOldestCacheEntryWhenLimitHasBeenReached() {
            // Given
            instanceProperties.setNumber(STATESTORE_PROVIDER_CACHE_SIZE, 2);
            TableProperties table1 = createTable("table-id-1", "test-table-1");
            StateStore store1 = createStateStore(table1);
            TableProperties table2 = createTable("table-id-2", "test-table-2");
            StateStore store2 = createStateStore(table2);
            TableProperties table3 = createTable("table-id-3", "test-table-3");
            StateStore store3 = createStateStore(table3);

            // When
            StateStoreProvider provider = provider();
            StateStore retrievedStore1 = provider.getStateStore(table1);
            StateStore retrievedStore2 = provider.getStateStore(table2);
            // Cache size has been reached, oldest state store will be removed from cache
            StateStore retrievedStore3 = provider.getStateStore(table3);
            StateStore retrievedStore4 = provider.getStateStore(table1);

            // Then
            assertThat(retrievedStore1).isSameAs(store1);
            assertThat(retrievedStore2).isSameAs(store2);
            assertThat(retrievedStore3).isSameAs(store3);
            assertThat(retrievedStore4).isSameAs(store1);
            assertThat(tablesLoaded).containsExactly(
                    "table-id-1",
                    "table-id-2",
                    "table-id-3",
                    "table-id-1");
        }

        @Test
        void shouldHaveNoLimitWhenCacheSizeIsNegative() {
            // Given
            instanceProperties.setNumber(STATESTORE_PROVIDER_CACHE_SIZE, -1);
            TableProperties table1 = createTable("table-id-1", "test-table-1");
            StateStore store1 = createStateStore(table1);
            TableProperties table2 = createTable("table-id-2", "test-table-2");
            StateStore store2 = createStateStore(table2);
            TableProperties table3 = createTable("table-id-3", "test-table-3");
            StateStore store3 = createStateStore(table3);

            // When
            StateStoreProvider provider = provider();
            StateStore retrievedStore1 = provider.getStateStore(table1);
            StateStore retrievedStore2 = provider.getStateStore(table2);
            StateStore retrievedStore3 = provider.getStateStore(table3);
            StateStore retrievedStore4 = provider.getStateStore(table1);

            // Then
            assertThat(retrievedStore1).isSameAs(store1);
            assertThat(retrievedStore2).isSameAs(store2);
            assertThat(retrievedStore3).isSameAs(store3);
            assertThat(retrievedStore4).isSameAs(store1);
            assertThat(tablesLoaded).containsExactly(
                    "table-id-1",
                    "table-id-2",
                    "table-id-3");
        }

        @Test
        void shouldRemoveLeastRecentlyUsedWhenOldestHasBeenUsedMoreRecently() {
            // Given
            instanceProperties.setNumber(STATESTORE_PROVIDER_CACHE_SIZE, 2);
            TableProperties table1 = createTable("table-id-1", "test-table-1");
            TableProperties table2 = createTable("table-id-2", "test-table-2");
            TableProperties table3 = createTable("table-id-3", "test-table-3");
            createStateStores(table1, table2, table3);

            // When
            StateStoreProvider provider = provider();
            provider.getStateStore(table1);
            provider.getStateStore(table2);
            provider.getStateStore(table1);
            // Cache size has been reached, oldest state store will be removed from cache
            provider.getStateStore(table3);
            provider.getStateStore(table1);
            provider.getStateStore(table2);

            // Then
            assertThat(tablesLoaded).containsExactly(
                    "table-id-1",
                    "table-id-2",
                    "table-id-3",
                    "table-id-2");
        }
    }

    @Nested
    @DisplayName("Remove a table from the cache")
    class RemoveFromCache {

        @Test
        void shouldNotRemoveFromCacheIfStateStoreNotLoaded() {
            // Given
            TableProperties table = createTable("test-table-id", "test-table");
            StateStore store = createStateStore(table);

            // When
            StateStoreProvider provider = provider();
            boolean removed = provider.removeStateStoreFromCache("test-table-id");

            // Then
            assertThat(tableIdToStateStore.get(table.get(TABLE_ID))).isSameAs(store);
            assertThat(tablesLoaded).isEmpty();
            assertThat(removed).isFalse();
        }

        @Test
        void shouldRemoveRequestedStateStoreFromCache() {
            // Given
            TableProperties table = createTable("test-table-id", "test-table");
            StateStore store = createStateStore(table);

            // When
            StateStoreProvider provider = provider();
            StateStore retrievedStore1 = provider.getStateStore(table);
            boolean removed = provider.removeStateStoreFromCache("test-table-id");
            // Table properties will be reloaded as no longer in cache
            StateStore retrievedStore2 = provider.getStateStore(table);

            // Then
            assertThat(retrievedStore1).isSameAs(store);
            assertThat(retrievedStore2).isSameAs(store);
            assertThat(removed).isTrue();
            assertThat(tablesLoaded).containsExactly("test-table-id", "test-table-id");
        }

        @Test
        void shouldRememberTableWasEvictedWhenRemovingOldestTableDueToMaxSize() {
            // Given
            instanceProperties.setNumber(STATESTORE_PROVIDER_CACHE_SIZE, 2);
            TableProperties table1 = createTable("table1", "test-table-1");
            TableProperties table2 = createTable("table2", "test-table-2");
            TableProperties table3 = createTable("table3", "test-table-3");
            createStateStores(table1, table2, table3);

            // And we reorder the table ages by evicting and re-adding
            StateStoreProvider provider = provider();
            provider.getStateStore(table1);
            provider.getStateStore(table2);
            provider.removeStateStoreFromCache("table1");
            provider.getStateStore(table1);

            // When we overflow the max size and then load the old tables again
            provider.getStateStore(table3);
            provider.getStateStore(table1);
            provider.getStateStore(table2);

            // Then the table that was evicted and re-added does not get evicted by the overflow
            assertThat(tablesLoaded).containsExactly(
                    "table1", "table2", "table1", "table3", "table2");
        }
    }

    @Nested
    @DisplayName("Free up heap space")
    class FreeUpHeapSpace {

        // TODO tests:
        // Remove least recently used table (distinguish from least recently added)
        // Don't remove specified table even if it's least recently used
        // Remove a different table when least recently used is required
        // Apply min free percentage as well as amount

        @Test
        void shouldFreeUpHeapSpaceByRemovingATableFromTheCache() {
            // Given
            instanceProperties.set(STATESTORE_PROVIDER_MIN_FREE_HEAP_TARGET_AMOUNT, "20");
            instanceProperties.set(STATESTORE_PROVIDER_MIN_FREE_HEAP_TARGET_PERCENTAGE, "10");
            TableProperties table = createTable("table", "my-table");
            createStateStores(table);
            provideMemoryStates(
                    initMaxMemory(100),
                    jvmAllocatedFreeAndMaxAllocated(90, 5, 100), // 15 total free memory
                    jvmAllocatedFreeAndMaxAllocated(90, 15, 100)); // 25 total free memory

            // When
            StateStoreProvider provider = provider();
            provider.getStateStore(table);
            provider.ensureEnoughHeapSpaceAvailable(Set.of());
            provider.getStateStore(table);

            // Then
            assertThat(tablesLoaded).containsExactly("table", "table");
        }

        @Test
        void shouldNotFreeUpHeapSpaceWhenEnoughIsFreeAlready() {
            // Given
            instanceProperties.set(STATESTORE_PROVIDER_MIN_FREE_HEAP_TARGET_AMOUNT, "20");
            instanceProperties.set(STATESTORE_PROVIDER_MIN_FREE_HEAP_TARGET_PERCENTAGE, "10");
            TableProperties table = createTable("table", "my-table");
            createStateStores(table);
            provideMemoryStates(
                    initMaxMemory(100),
                    jvmAllocatedFreeAndMaxAllocated(90, 15, 100)); // 25 total free memory

            // When
            StateStoreProvider provider = provider();
            provider.getStateStore(table);
            provider.ensureEnoughHeapSpaceAvailable(Set.of());
            provider.getStateStore(table);

            // Then
            assertThat(tablesLoaded).containsExactly("table");
        }

        @Test
        void shouldNotFreeUpHeapSpaceWhenTooMuchIsUsedButCacheIsAlreadyEmpty() {
            // Given
            instanceProperties.set(STATESTORE_PROVIDER_MIN_FREE_HEAP_TARGET_AMOUNT, "20");
            instanceProperties.set(STATESTORE_PROVIDER_MIN_FREE_HEAP_TARGET_PERCENTAGE, "10");
            TableProperties table = createTable("table", "my-table");
            createStateStores(table);
            fixMemoryState(jvmAllocatedFreeAndMaxAllocated(90, 5, 100)); // 15 total free memory

            // When / Then
            StateStoreProvider provider = provider();
            assertThatCode(() -> provider.ensureEnoughHeapSpaceAvailable(Set.of()))
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldNotFreeUpHeapSpaceWhenTooMuchIsUsedButTableIsRequired() {
            // Given
            instanceProperties.set(STATESTORE_PROVIDER_MIN_FREE_HEAP_TARGET_AMOUNT, "20");
            instanceProperties.set(STATESTORE_PROVIDER_MIN_FREE_HEAP_TARGET_PERCENTAGE, "10");
            TableProperties table = createTable("table", "my-table");
            createStateStores(table);
            fixMemoryState(jvmAllocatedFreeAndMaxAllocated(90, 5, 100)); // 15 total free memory

            // When
            StateStoreProvider provider = provider();
            provider.getStateStore(table);
            provider.ensureEnoughHeapSpaceAvailable(Set.of("table"));
            provider.getStateStore(table);

            // Then
            assertThat(tablesLoaded).containsExactly("table");
        }

        @Test
        void shouldRemoveOtherTableWhenLeastRecentlyUsedIsRequired() {
            // Given
            instanceProperties.set(STATESTORE_PROVIDER_MIN_FREE_HEAP_TARGET_AMOUNT, "20");
            instanceProperties.set(STATESTORE_PROVIDER_MIN_FREE_HEAP_TARGET_PERCENTAGE, "10");
            TableProperties table1 = createTable("table1", "test-table-1");
            TableProperties table2 = createTable("table2", "test-table-2");
            createStateStores(table1, table2);
            provideMemoryStates(
                    initMaxMemory(100),
                    jvmAllocatedFreeAndMaxAllocated(90, 5, 100), // 15 total free memory
                    jvmAllocatedFreeAndMaxAllocated(90, 15, 100)); // 25 total free memory

            // When
            StateStoreProvider provider = provider();
            provider.getStateStore(table1);
            provider.getStateStore(table2);
            provider.ensureEnoughHeapSpaceAvailable(Set.of("table1"));
            provider.getStateStore(table1);
            provider.getStateStore(table2);

            // Then
            assertThat(tablesLoaded).containsExactly("table1", "table2", "table2");
        }

        @Test
        void shouldRemoveLeastRecentlyUsedButNotOldestTableWhenLeastRecentlyUsedIsRequired() {
            // Given
            instanceProperties.set(STATESTORE_PROVIDER_MIN_FREE_HEAP_TARGET_AMOUNT, "20");
            instanceProperties.set(STATESTORE_PROVIDER_MIN_FREE_HEAP_TARGET_PERCENTAGE, "10");
            TableProperties table1 = createTable("table1", "test-table-1");
            TableProperties table2 = createTable("table2", "test-table-2");
            TableProperties table3 = createTable("table3", "test-table-3");
            createStateStores(table1, table2, table3);
            provideMemoryStates(
                    initMaxMemory(100),
                    jvmAllocatedFreeAndMaxAllocated(90, 5, 100), // 15 total free memory
                    jvmAllocatedFreeAndMaxAllocated(90, 15, 100)); // 25 total free memory

            // When
            StateStoreProvider provider = provider();
            provider.getStateStore(table1);
            provider.getStateStore(table2);
            provider.getStateStore(table3);
            provider.getStateStore(table2);
            provider.ensureEnoughHeapSpaceAvailable(Set.of("table1"));
            provider.getStateStore(table1);
            provider.getStateStore(table2);
            provider.getStateStore(table3);

            // Then
            assertThat(tablesLoaded).containsExactly("table1", "table2", "table3", "table3");
        }
    }

    private TableProperties createTable(String tableId, String tableName) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, tableId);
        tableProperties.set(TABLE_NAME, tableName);
        return tableProperties;
    }

    private void createStateStores(TableProperties... tables) {
        for (TableProperties table : tables) {
            createStateStore(table);
        }
    }

    private StateStore createStateStore(TableProperties table) {
        StateStore stateStore = InMemoryTransactionLogStateStore.createAndInitialise(table, new InMemoryTransactionLogs());
        tableIdToStateStore.put(table.get(TABLE_ID), stateStore);
        return stateStore;
    }

    private StateStoreProvider provider() {
        return new StateStoreProvider(instanceProperties, tableProperties -> {
            tablesLoaded.add(tableProperties.get(TABLE_ID));
            return tableIdToStateStore.get(tableProperties.get(TABLE_ID));
        }, memoryProvider);
    }

    private JvmMemoryUse initMaxMemory(long maxMemory) {
        return new JvmMemoryUse(0, 0, maxMemory);
    }

    private JvmMemoryUse jvmAllocatedFreeAndMaxAllocated(long totalMemory, long freeMemory, long maxMemory) {
        return new JvmMemoryUse(totalMemory, freeMemory, maxMemory);
    }

    private void provideMemoryStates(JvmMemoryUse... states) {
        memoryProvider = List.of(states).iterator()::next;
    }

    private void fixMemoryState(JvmMemoryUse state) {
        memoryProvider = () -> state;
    }
}
