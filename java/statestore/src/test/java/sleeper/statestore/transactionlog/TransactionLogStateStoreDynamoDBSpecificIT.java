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
package sleeper.statestore.transactionlog;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.StateStore;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class TransactionLogStateStoreDynamoDBSpecificIT extends TransactionLogStateStoreTestBase {
    private final Schema schema = schemaWithKey("key", new LongType());

    @Test
    void shouldInitialiseTableWithManyPartitionsCreatingTransactionTooLargeToFitInADynamoDBItem() throws Exception {
        // Given
        StateStore stateStore = getTableStateStore();
        List<String> leafIds = IntStream.range(0, 1000)
                .mapToObj(i -> "" + i)
                .collect(toUnmodifiableList());
        List<Object> splitPoints = LongStream.range(1, 1000)
                .mapToObj(i -> i)
                .collect(toUnmodifiableList());
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(leafIds, splitPoints)
                .anyTreeJoiningAllLeaves().buildTree();

        // When
        stateStore.initialise(tree.getAllPartitions());

        // Then
        assertThat(stateStore.getAllPartitions()).containsExactlyElementsOf(tree.getAllPartitions());
    }

    private StateStore getTableStateStore() {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        return DynamoDBTransactionLogStateStore.builderFrom(instanceProperties, tableProperties, dynamoDBClient, s3Client)
                .maxAddTransactionAttempts(1)
                .build();
    }
}
