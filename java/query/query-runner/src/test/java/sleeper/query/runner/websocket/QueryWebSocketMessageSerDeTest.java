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
package sleeper.query.runner.websocket;

import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.query.core.output.ResultsOutputLocation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class QueryWebSocketMessageSerDeTest {

    @Nested
    @DisplayName("Status messages")
    class StatusMessages {

        QueryWebSocketMessageSerDe serDe = QueryWebSocketMessageSerDe.forStatusMessages();

        @Test
        void shouldSerDeQueryWasSplitToSubqueries() {
            // Given
            QueryWebSocketMessage message = QueryWebSocketMessage.queryWasSplitToSubqueries("test-query", List.of("subquery-1", "subquery-2"));

            // When
            String json = serDe.toJsonPrettyPrint(message);
            QueryWebSocketMessage found = serDe.fromJson(json);

            // Then
            assertThat(found).isEqualTo(message);
            Approvals.verify(json, new Options().forFile().withExtension(".json"));
        }

        @Test
        void shouldSerDeQueryCompleted() {
            // Given
            QueryWebSocketMessage message = QueryWebSocketMessage.queryCompleted("test-query", 123,
                    List.of(new ResultsOutputLocation("s3", "s3a://test-bucket/test-file.parquet")));

            // When
            String json = serDe.toJsonPrettyPrint(message);
            QueryWebSocketMessage found = serDe.fromJson(json);

            // Then
            assertThat(found).isEqualTo(message);
            Approvals.verify(json, new Options().forFile().withExtension(".json"));
        }

        @Test
        void shouldSerDeQueryError() {
            // Given
            QueryWebSocketMessage message = QueryWebSocketMessage.queryError(
                    "test-query", "Something went wrong", 123,
                    List.of(new ResultsOutputLocation("s3", "s3a://test-bucket/test-file.parquet")));

            // When
            String json = serDe.toJsonPrettyPrint(message);
            QueryWebSocketMessage found = serDe.fromJson(json);

            // Then
            assertThat(found).isEqualTo(message);
            Approvals.verify(json, new Options().forFile().withExtension(".json"));
        }
    }

    @Nested
    @DisplayName("Row batch message")
    class Rows {
        Schema schema = createSchemaWithKey("key", new LongType());
        QueryWebSocketMessageSerDe serDe = QueryWebSocketMessageSerDe.forBatchSizeAndPayloadSize(null, 1000, schema);

        @Test
        void shouldSerDeRows() throws Exception {
            // Given
            List<Row> rows = List.of(
                    new Row(Map.of("key", 123L)),
                    new Row(Map.of("key", 12345L)));

            // When
            List<String> json = toJson(serDe, "test-query", rows);
            List<QueryWebSocketMessage> found = json.stream().map(serDe::fromJson).toList();

            // Then
            assertThat(found).containsExactly(
                    QueryWebSocketMessage.rowsBatch("test-query", rows));
            Approvals.verify(json);
        }

        @Test
        void shouldCountRowsSent() throws Exception {
            // Given
            List<Row> rows = List.of(
                    new Row(Map.of("key", 10L)),
                    new Row(Map.of("key", 20L)));

            // When
            long count = serDe.forEachRowBatchJson("test-query", rows.iterator(), json -> {
            });

            // Then
            assertThat(count).isEqualTo(2L);
        }

        @Test
        void shouldCountRowsSentWhenSendingFails() {
            // Given
            List<Row> rows = List.of(
                    new Row(Map.of("key", 10L)),
                    new Row(Map.of("key", 20L)),
                    new Row(Map.of("key", 30L)));
            RuntimeException failure = new RuntimeException("Unexpected failure");
            Consumer<String> handler = json -> {
                throw failure;
            };

            // When
            assertThatThrownBy(() -> serDe.forEachRowBatchJson("test-query", rows.iterator(), handler))
                    .isInstanceOfSatisfying(QueryWebSocketRowsException.class,
                            e -> assertThat(e.getRecordsSent()).isEqualTo(0L))
                    .hasCause(failure);
        }
    }

    @Nested
    @DisplayName("Split up row batches by number of rows")
    class RowBatchSize {
        Schema schema = createSchemaWithKey("key", new LongType());

        private QueryWebSocketMessageSerDe serDeForBatchSize(int batchSize) {
            return QueryWebSocketMessageSerDe.forBatchSizeAndPayloadSize(batchSize, 1000, schema);
        }

        @Test
        void shouldSerDeTwoBatches() throws Exception {
            // Given
            List<Row> rows = List.of(
                    new Row(Map.of("key", 123L)),
                    new Row(Map.of("key", 12345L)));
            QueryWebSocketMessageSerDe serDe = serDeForBatchSize(1);

            // When
            List<String> json = toJson(serDe, "test-query", rows);
            List<QueryWebSocketMessage> found = json.stream().map(serDe::fromJson).toList();

            // Then
            assertThat(found).containsExactly(
                    QueryWebSocketMessage.rowsBatch("test-query", List.of(new Row(Map.of("key", 123L)))),
                    QueryWebSocketMessage.rowsBatch("test-query", List.of(new Row(Map.of("key", 12345L)))));
        }

        @Test
        void shouldSerDePartialBatchAtEnd() throws Exception {
            // Given
            List<Row> rows = List.of(
                    new Row(Map.of("key", 10L)),
                    new Row(Map.of("key", 20L)),
                    new Row(Map.of("key", 30L)));
            QueryWebSocketMessageSerDe serDe = serDeForBatchSize(2);

            // When
            List<String> json = toJson(serDe, "test-query", rows);
            List<QueryWebSocketMessage> found = json.stream().map(serDe::fromJson).toList();

            // Then
            assertThat(found).containsExactly(
                    QueryWebSocketMessage.rowsBatch("test-query", List.of(
                            new Row(Map.of("key", 10L)),
                            new Row(Map.of("key", 20L)))),
                    QueryWebSocketMessage.rowsBatch("test-query", List.of(
                            new Row(Map.of("key", 30L)))));
        }

        @Test
        void shouldCountRowsSent() throws Exception {
            // Given
            List<Row> rows = List.of(
                    new Row(Map.of("key", 10L)),
                    new Row(Map.of("key", 20L)),
                    new Row(Map.of("key", 30L)),
                    new Row(Map.of("key", 40L)),
                    new Row(Map.of("key", 50L)));
            QueryWebSocketMessageSerDe serDe = serDeForBatchSize(3);

            // When
            long count = serDe.forEachRowBatchJson("test-query", rows.iterator(), json -> {
            });

            // Then
            assertThat(count).isEqualTo(5L);
        }

        @Test
        void shouldCountRowsSentWhenSendingLastBatchFails() {
            // Given
            List<Row> rows = List.of(
                    new Row(Map.of("key", 10L)),
                    new Row(Map.of("key", 20L)),
                    new Row(Map.of("key", 30L)));
            QueryWebSocketMessageSerDe serDe = serDeForBatchSize(1);
            RuntimeException failure = new RuntimeException("Unexpected failure");
            AtomicInteger numBatches = new AtomicInteger(0);
            Consumer<String> handler = json -> {
                if (numBatches.incrementAndGet() == 3) {
                    throw failure;
                }
            };

            // When
            assertThatThrownBy(() -> serDe.forEachRowBatchJson("test-query", rows.iterator(), handler))
                    .isInstanceOfSatisfying(QueryWebSocketRowsException.class,
                            e -> assertThat(e.getRecordsSent()).isEqualTo(2L))
                    .hasCause(failure);
        }
    }

    @Nested
    @DisplayName("Split up row batches by size of message payload")
    class RowPayloadSize {
        Schema schema = createSchemaWithKey("key", new LongType());

        private QueryWebSocketMessageSerDe serDeForPayloadSize(int payloadSize) {
            return QueryWebSocketMessageSerDe.forBatchSizeAndPayloadSize(null, payloadSize, schema);
        }

        @Test
        void shouldSerDeTwoBatches() throws Exception {
            // Given
            List<Row> rows = List.of(
                    new Row(Map.of("key", 123L)),
                    new Row(Map.of("key", 12345L)));
            QueryWebSocketMessageSerDe serDe = serDeForPayloadSize(60);

            // When
            List<String> json = toJson(serDe, "test-query", rows);
            List<QueryWebSocketMessage> found = json.stream().map(serDe::fromJson).toList();

            // Then
            assertThat(found).containsExactly(
                    QueryWebSocketMessage.rowsBatch("test-query", List.of(new Row(Map.of("key", 123L)))),
                    QueryWebSocketMessage.rowsBatch("test-query", List.of(new Row(Map.of("key", 12345L)))));
        }

        @Test
        void shouldSerDeTwoBatchesByPayloadSizeLessThanBaseMessageLength() throws Exception {
            // Given
            List<Row> rows = List.of(
                    new Row(Map.of("key", 123L)),
                    new Row(Map.of("key", 12345L)));
            QueryWebSocketMessageSerDe serDe = serDeForPayloadSize(10);

            // When
            List<String> json = toJson(serDe, "test-query", rows);
            List<QueryWebSocketMessage> found = json.stream().map(serDe::fromJson).toList();

            // Then
            assertThat(found).containsExactly(
                    QueryWebSocketMessage.rowsBatch("test-query", List.of(new Row(Map.of("key", 123L)))),
                    QueryWebSocketMessage.rowsBatch("test-query", List.of(new Row(Map.of("key", 12345L)))));
        }

        @Test
        void shouldResetRemainingSizeBetweenBatches() throws Exception {
            // Given
            List<Row> rows = List.of(
                    new Row(Map.of("key", 10L)),
                    new Row(Map.of("key", 20L)),
                    new Row(Map.of("key", 30L)),
                    new Row(Map.of("key", 40L)),
                    new Row(Map.of("key", 50L)));
            QueryWebSocketMessageSerDe serDe = serDeForPayloadSize(70);

            // When
            List<String> json = toJson(serDe, "test-query", rows);
            List<QueryWebSocketMessage> found = json.stream().map(serDe::fromJson).toList();

            // Then
            assertThat(found).containsExactly(
                    QueryWebSocketMessage.rowsBatch("test-query", List.of(
                            new Row(Map.of("key", 10L)),
                            new Row(Map.of("key", 20L)))),
                    QueryWebSocketMessage.rowsBatch("test-query", List.of(
                            new Row(Map.of("key", 30L)),
                            new Row(Map.of("key", 40L)))),
                    QueryWebSocketMessage.rowsBatch("test-query", List.of(
                            new Row(Map.of("key", 50L)))));
        }

        @Test
        void shouldResetRemainingSizeBetweenBatchesWhenSizeIsMetByNumberOfRecords() throws Exception {
            // Given
            List<Row> rows = List.of(
                    new Row(Map.of("key", 10L)),
                    new Row(Map.of("key", 20L)),
                    new Row(Map.of("key", 30L)),
                    new Row(Map.of("key", 40L)),
                    new Row(Map.of("key", 50L)));
            QueryWebSocketMessageSerDe serDe = QueryWebSocketMessageSerDe.forBatchSizeAndPayloadSize(2, 70, schema);

            // When
            List<String> json = toJson(serDe, "test-query", rows);
            List<QueryWebSocketMessage> found = json.stream().map(serDe::fromJson).toList();

            // Then
            assertThat(found).containsExactly(
                    QueryWebSocketMessage.rowsBatch("test-query", List.of(
                            new Row(Map.of("key", 10L)),
                            new Row(Map.of("key", 20L)))),
                    QueryWebSocketMessage.rowsBatch("test-query", List.of(
                            new Row(Map.of("key", 30L)),
                            new Row(Map.of("key", 40L)))),
                    QueryWebSocketMessage.rowsBatch("test-query", List.of(
                            new Row(Map.of("key", 50L)))));
        }
    }

    private List<String> toJson(QueryWebSocketMessageSerDe serDe, String queryId, List<Row> rows) throws Exception {
        List<String> json = new ArrayList<>();
        serDe.forEachRowBatchJson("test-query", rows.iterator(), json::add);
        return json;
    }

}
