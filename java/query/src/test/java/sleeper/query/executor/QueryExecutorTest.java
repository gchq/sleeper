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
package sleeper.query.executor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.StateStore;
import sleeper.query.recordretrieval.InMemoryLeafPartitionRecordRetriever;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.QUERY_PROCESSOR_CACHE_TIMEOUT;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithSinglePartition;


public class QueryExecutorTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final InMemoryLeafPartitionRecordRetriever recordStore = new InMemoryLeafPartitionRecordRetriever();

    @Nested
    @DisplayName("Reinitialise based on a timeout")
    class ReinitialiseOnTimeout {
        private final Schema schema = schemaWithKey("key");
        private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        private final StateStore stateStore = inMemoryStateStoreWithSinglePartition(schema);
        private final FileInfoFactory fileInfoFactory = FileInfoFactory.from(schema, stateStore);

        @Test
        public void shouldReloadActiveFilesFromStateStoreWhenTimedOut() throws Exception {
            // Given
            tableProperties.set(QUERY_PROCESSOR_CACHE_TIMEOUT, "5");
            QueryExecutor queryExecutor = executorAtTime(Instant.parse("2023-11-24T15:59:00Z"));

            // When 1
            stateStore.addFile(fileInfoFactory.rootFile(1));

            // Then 1
            assertThat(queryExecutor.cacheRefreshRequired()).isTrue();

            // When 2
            queryExecutor.init();

            // Then 2
            assertThat(queryExecutor.cacheRefreshRequired()).isFalse();
        }

        @Test
        public void shouldNotReloadActiveFilesBeforeTimeOut() throws Exception {
            // Given
            tableProperties.set(QUERY_PROCESSOR_CACHE_TIMEOUT, "5");
            QueryExecutor queryExecutor = executorAtTime(Instant.parse("2023-11-24T15:59:00Z"));

            // When 1
            stateStore.addFile(fileInfoFactory.rootFile(1));

            // Then 1
            assertThat(queryExecutor.cacheRefreshRequired()).isTrue();

            // When 2
            queryExecutor.init();
            queryExecutor.setCacheExpireTime(Instant.now().minus(5, ChronoUnit.MINUTES));

            // Then 2
            assertThat(queryExecutor.cacheRefreshRequired()).isTrue();
        }

        private QueryExecutor executorAtTime(Instant timeNow) {
            return new QueryExecutor(ObjectFactory.noUserJars(), stateStore, tableProperties, recordStore, timeNow);
        }
    }
}
