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

package sleeper.statestore.dynamodb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.AssignJobToFilesRequest;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.AssignJobsToFilesStateStoreAdapter;
import sleeper.statestore.FixedStateStoreProvider;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class DynamoDBAssignJobsToFilesIT extends DynamoDBStateStoreTestBase {

    Instant updateTime = Instant.parse("2023-10-04T14:08:00Z");
    final Schema schema = schemaWithKey("key", new LongType());
    final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    final PartitionsBuilder partitions = new PartitionsBuilder(schema).singlePartition("root");
    StateStore stateStore;
    FileInfoFactory factory = FileInfoFactory.fromUpdatedAt(partitions.buildTree(), updateTime);

    @BeforeEach
    void setUpTable() {
        stateStore = new DynamoDBStateStore(instanceProperties, tableProperties, dynamoDBClient);
        stateStore.fixTime(updateTime);
    }

    @Test
    void shouldAssignJobToFile() throws Exception {
        // Given
        FileInfo file = factory.rootFile("test.parquet", 100);
        stateStore.addFile(file);

        // When
        assignJobs().updateJobStatusOfFiles(List.of(
                rootRequest().jobId("test-job").files(List.of("test.parquet")).build()));

        // Then
        assertThat(stateStore.getActiveFilesWithNoJobId()).isEmpty();
        assertThat(stateStore.getActiveFiles()).containsExactly(
                file.toBuilder().jobId("test-job").build());
    }

    private AssignJobToFilesRequest.Client assignJobs() {
        return new AssignJobsToFilesStateStoreAdapter(
                new FixedTablePropertiesProvider(tableProperties),
                new FixedStateStoreProvider(tableProperties, stateStore));
    }

    private AssignJobToFilesRequest.Builder rootRequest() {
        return AssignJobToFilesRequest.builder()
                .tableId(tableProperties.get(TABLE_ID))
                .partitionId("root");
    }
}
