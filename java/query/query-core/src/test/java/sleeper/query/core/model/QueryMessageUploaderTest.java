/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.query.core.model;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class QueryMessageUploaderTest {

    private static final int LENGTH_TO_STORE = 1000;

    private final Schema schema = createSchemaWithKey("key", new LongType());
    private final RangeFactory rangeFactory = new RangeFactory(schema);
    private final PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
    private final QuerySerDe querySerDe = new QuerySerDe(schema);
    private final InMemoryQueryJsonStore store = new InMemoryQueryJsonStore();
    private final QueryMessageUploader uploader = new QueryMessageUploader(store, querySerDe, LENGTH_TO_STORE, () -> "test-uuid");
    private final QueryMessageReader reader = new QueryMessageReader(store, querySerDe);

    @Test
    void shouldHoldSmallQueryInMessage() {
        // Given
        Query query = queryWithRegions(1);

        // When
        String message = uploader.serialiseAndUploadIfTooBig(query);

        // Then
        assertThat(message).isEqualTo(querySerDe.toJson(query));
        assertThat(store.getJsonByKey()).isEmpty();
        assertThat(reader.readAndLoadIfStored(message).asParentQuery()).isEqualTo(query);
    }

    @Test
    void shouldUploadLargeQuery() {
        // Given
        Query query = queryWithRegions(100);

        // When
        String message = uploader.serialiseAndUploadIfTooBig(query);

        // Then
        assertThat(message).isEqualTo("{\"queryId\":\"test-query\",\"queryJsonS3Key\":\"queries/test-query/query-test-uuid.json\"}");
        assertThat(store.getJsonByKey()).isEqualTo(Map.of(
                "queries/test-query/query-test-uuid.json", querySerDe.toJson(query)));
        assertThat(reader.readAndLoadIfStored(message).asParentQuery()).isEqualTo(query);
    }

    @Test
    void shouldHoldSmallLeafQueryInMessage() {
        // Given
        LeafPartitionQuery query = leafQueryWithFiles(1);

        // When
        String message = uploader.serialiseAndUploadIfTooBig(query);

        // Then
        assertThat(message).isEqualTo(querySerDe.toJson(query));
        assertThat(store.getJsonByKey()).isEmpty();
        assertThat(reader.readAndLoadIfStored(message).asLeafQuery()).isEqualTo(query);
    }

    @Test
    void shouldUploadLargeLeafQuery() {
        // Given
        LeafPartitionQuery query = leafQueryWithFiles(100);

        // When
        String message = uploader.serialiseAndUploadIfTooBig(query);

        // Then
        assertThat(message).isEqualTo("{\"queryId\":\"test-query\",\"queryJsonS3Key\":\"queries/test-query/test-subquery-test-uuid.json\"}");
        assertThat(store.getJsonByKey()).containsOnlyKeys("queries/test-query/test-subquery-test-uuid.json");
        assertThat(reader.readAndLoadIfStored(message).asLeafQuery()).isEqualTo(query);
    }

    @Test
    void shouldFailToReadWhenStoredQueryIsMissing() {
        // Given
        String message = querySerDe.toJsonPointingToStoredQuery("test-query", "queries/missing.json");

        // When / Then
        assertThatThrownBy(() -> reader.readAndLoadIfStored(message))
                .hasMessageContaining("queries/missing.json");
    }

    private Query queryWithRegions(int numRegions) {
        return Query.builder()
                .queryId("test-query")
                .tableName("test-table")
                .regions(IntStream.range(0, numRegions)
                        .mapToObj(i -> new Region(rangeFactory.createRange("key", i * 10L, i * 10L + 5)))
                        .toList())
                .build();
    }

    private LeafPartitionQuery leafQueryWithFiles(int numFiles) {
        return LeafPartitionQuery.builder()
                .queryId("test-query")
                .subQueryId("test-subquery")
                .tableId("test-table-id")
                .leafPartitionId("root")
                .regions(List.of(new Region(rangeFactory.createRange("key", 10L, 20L))))
                .partitionRegion(partitions.getRootPartition().getRegion())
                .files(IntStream.range(0, numFiles)
                        .mapToObj(i -> "s3a://data-bucket/table/data/partition_root/file-" + i + ".parquet")
                        .toList())
                .processingConfig(QueryProcessingConfig.none())
                .build();
    }
}
