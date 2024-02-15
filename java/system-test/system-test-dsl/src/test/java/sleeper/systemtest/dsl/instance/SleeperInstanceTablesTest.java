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

package sleeper.systemtest.dsl.instance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.withDefaultProperties;

@InMemoryDslTest
public class SleeperInstanceTablesTest {
    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstanceNoTables(withDefaultProperties("main"));
    }

    @Test
    void shouldCreateTwoTablesWithDifferentPartitionsAndSchemas(SleeperSystemTest sleeper) {
        // Given
        Schema schemaA = schemaWithKey("keyA", new StringType());
        Schema schemaB = schemaWithKey("keyA", new LongType());
        PartitionTree partitionsA = new PartitionsBuilder(schemaA)
                .rootFirst("A-root")
                .splitToNewChildren("A-root", "AL", "AR", "aaa")
                .buildTree();
        PartitionTree partitionsB = new PartitionsBuilder(schemaB)
                .rootFirst("B-root")
                .splitToNewChildren("B-root", "BL", "BR", 50L)
                .buildTree();
        sleeper.tables()
                .create("A", schemaA)
                .create("B", schemaB);

        // When
        sleeper.tables().activate("A");
        sleeper.partitioning().setPartitions(partitionsA);
        sleeper.tables().activate("B");
        sleeper.partitioning().setPartitions(partitionsB);

        // Then
        assertThat(sleeper.partitioning().treeByTable())
                .isEqualTo(Map.of("A", partitionsA, "B", partitionsB));
    }

    @Test
    void shouldSetPartitionsForMultipleTables(SleeperSystemTest sleeper) {
        // Given
        Schema schema = schemaWithKey("key", new StringType());
        sleeper.tables().create(List.of("A", "B"), schema);
        PartitionTree partitions = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "aaa")
                .buildTree();

        // When
        sleeper.tables().forEach(() ->
                sleeper.partitioning().setPartitions(partitions));

        // Then
        assertThat(sleeper.partitioning().treeByTable())
                .isEqualTo(Map.of("A", partitions, "B", partitions));
    }
}
