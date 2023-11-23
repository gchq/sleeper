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

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.StateStore;
import sleeper.query.recordretrieval.InMemoryLeafPartitionRecordRetriever;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TableProperty.QUERY_PROCESSOR_CACHE_TIMEOUT;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithPartitions;


public class QueryExecutorTest {

    @Test
    public void shouldNotRequireCacheRest() throws Exception {
        // Given
        InMemoryLeafPartitionRecordRetriever leafPartitionRecordRetriever = new InMemoryLeafPartitionRecordRetriever();

        Schema schema = schemaWithKey("key");
        InstanceProperties instanceProperties = createTestInstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        tableProperties.set(QUERY_PROCESSOR_CACHE_TIMEOUT, "5");
        StateStore stateStore = inMemoryStateStoreWithPartitions(new PartitionsBuilder(schema).rootFirst("root").buildList());
        QueryExecutor queryExecutor = new QueryExecutor(tableProperties, stateStore, leafPartitionRecordRetriever);
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
            .schema(schema)
            .partitions(stateStore.getAllPartitions())
            .build();

        // When 1
        stateStore.initialise(stateStore.getAllPartitions());
        stateStore.addFile(fileInfoFactory.rootFile(1));

        // Then 1
        assertThat(queryExecutor.cacheRefreshRequired()).isTrue();

        // When 2
        queryExecutor.init();

        // Then 2
        assertThat(queryExecutor.cacheRefreshRequired()).isFalse();
    }

    @Test
    public void shouldRequireCacheRest() throws Exception {
        // Given
        InMemoryLeafPartitionRecordRetriever leafPartitionRecordRetriever = new InMemoryLeafPartitionRecordRetriever();

        Schema schema = schemaWithKey("key");
        InstanceProperties instanceProperties = createTestInstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        tableProperties.set(QUERY_PROCESSOR_CACHE_TIMEOUT, "5");
        StateStore stateStore = inMemoryStateStoreWithPartitions(new PartitionsBuilder(schema).rootFirst("root").buildList());
        QueryExecutor queryExecutor = new QueryExecutor(tableProperties, stateStore, leafPartitionRecordRetriever);
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
            .schema(schema)
            .partitions(stateStore.getAllPartitions())
            .build();

        // When 1
        stateStore.initialise(stateStore.getAllPartitions());
        stateStore.addFile(fileInfoFactory.rootFile(1));

        // Then 1
        assertThat(queryExecutor.cacheRefreshRequired()).isTrue();

        // When 2
        queryExecutor.init();
        queryExecutor.setCacheExpireTime(Instant.now().minus(5, ChronoUnit.MINUTES));

        // Then 2
        assertThat(queryExecutor.cacheRefreshRequired()).isTrue();
    }
}
