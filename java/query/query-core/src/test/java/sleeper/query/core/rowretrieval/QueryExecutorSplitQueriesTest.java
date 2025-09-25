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
package sleeper.query.core.rowretrieval;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.range.Region;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class QueryExecutorSplitQueriesTest extends QueryExecutorTestBase {

    @Test
    void shouldCreateNoSubqueriesWhenNoFilesArePresent() throws Exception {
        // Given
        Query query = queryRange(1L, 10L);

        // When / Then
        assertThat(executor().splitIntoLeafPartitionQueries(query))
                .isEmpty();
    }

    @Test
    void shouldCreateOneSubqueryWithOneFileInOnePartition() throws Exception {
        // Given
        addRootFile("test.parquet", List.of(new Row(Map.of("key", 1L))));
        Region region = range(0L, 10L);
        Query query = queryRegions(region);

        // When / Then
        assertThat(executor().splitIntoLeafPartitionQueries(query))
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("subQueryId")
                .containsExactly(LeafPartitionQuery.builder()
                        .parentQuery(query)
                        .tableId(tableProperties.get(TABLE_ID))
                        .subQueryId("ignored")
                        .regions(List.of(region))
                        .leafPartitionId("root")
                        .partitionRegion(rootPartitionRegion())
                        .files(List.of("test.parquet"))
                        .build());
    }

    @Test
    void shouldCreateOneSubqueryWithMultipleFilesInOnePartition() throws Exception {
        // Given
        addRootFile("file1.parquet", List.of(new Row(Map.of("key", 1L))));
        addRootFile("file2.parquet", List.of(new Row(Map.of("key", 2L))));
        addRootFile("file3.parquet", List.of(new Row(Map.of("key", 3L))));
        Region region = range(0L, 5L);
        Query query = queryRegions(region);

        // When / Then
        assertThat(executor().splitIntoLeafPartitionQueries(query))
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("subQueryId")
                .containsExactly(LeafPartitionQuery.builder()
                        .parentQuery(query)
                        .tableId(tableProperties.get(TABLE_ID))
                        .subQueryId("ignored")
                        .regions(List.of(region))
                        .leafPartitionId("root")
                        .partitionRegion(rootPartitionRegion())
                        .files(List.of("file1.parquet", "file2.parquet", "file3.parquet"))
                        .build());
    }

    @Test
    void shouldCreateTwoSubqueriesWithTwoLeafPartitions() throws Exception {
        // Given
        PartitionTree tree = new PartitionsBuilder(tableProperties)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 5L)
                .buildTree();
        update(stateStore).initialise(tree);
        addPartitionFile("left", "left1.parquet", List.of(new Row(Map.of("key", 1L))));
        addPartitionFile("left", "left2.parquet", List.of(new Row(Map.of("key", 1L))));
        addPartitionFile("right", "right1.parquet", List.of(new Row(Map.of("key", 2L))));
        addPartitionFile("right", "right2.parquet", List.of(new Row(Map.of("key", 2L))));
        Region region = range(0L, 10L);
        Query query = queryRegions(region);

        // When / Then
        assertThat(executor().splitIntoLeafPartitionQueries(query))
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("subQueryId")
                .containsExactlyInAnyOrder(
                        LeafPartitionQuery.builder()
                                .parentQuery(query)
                                .tableId(tableProperties.get(TABLE_ID))
                                .subQueryId("ignored")
                                .regions(List.of(region))
                                .leafPartitionId("left")
                                .partitionRegion(tree.getPartition("left").getRegion())
                                .files(List.of("left1.parquet", "left2.parquet"))
                                .build(),
                        LeafPartitionQuery.builder()
                                .parentQuery(query)
                                .tableId(tableProperties.get(TABLE_ID))
                                .subQueryId("ignored")
                                .regions(List.of(region))
                                .leafPartitionId("right")
                                .partitionRegion(tree.getPartition("right").getRegion())
                                .files(List.of("right1.parquet", "right2.parquet"))
                                .build());
    }

    @Test
    void shouldCreateTwoSubqueriesWithMultidimensionalKeyAndOneSplitPoint() throws Exception {
        // Given
        tableProperties.setSchema(Schema.builder()
                .rowKeyFields(
                        new Field("key1", new LongType()),
                        new Field("key2", new StringType()))
                .build());
        PartitionTree tree = new PartitionsBuilder(tableProperties)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 5L)
                .buildTree();
        update(stateStore).initialise(tree);
        addPartitionFile("left", "left.parquet", List.of(new Row(Map.of("key1", 1L, "key2", "A"))));
        addPartitionFile("right", "right.parquet", List.of(new Row(Map.of("key1", 2L, "key2", "B"))));
        Region region = new Region(List.of(
                rangeFactory().createRange("key1", 0L, 10L),
                rangeFactory().createRange("key2", "A", "Z")));
        Query query = queryRegions(region);

        // When / Then
        assertThat(executor().splitIntoLeafPartitionQueries(query))
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("subQueryId")
                .containsExactlyInAnyOrder(
                        LeafPartitionQuery.builder()
                                .parentQuery(query)
                                .tableId(tableProperties.get(TABLE_ID))
                                .subQueryId("ignored")
                                .regions(List.of(region))
                                .leafPartitionId("left")
                                .partitionRegion(tree.getPartition("left").getRegion())
                                .files(List.of("left.parquet"))
                                .build(),
                        LeafPartitionQuery.builder()
                                .parentQuery(query)
                                .tableId(tableProperties.get(TABLE_ID))
                                .subQueryId("ignored")
                                .regions(List.of(region))
                                .leafPartitionId("right")
                                .partitionRegion(tree.getPartition("right").getRegion())
                                .files(List.of("right.parquet"))
                                .build());
    }

}
