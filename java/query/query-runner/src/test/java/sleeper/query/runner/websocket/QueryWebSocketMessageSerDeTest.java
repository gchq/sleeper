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

import static org.assertj.core.api.Assertions.assertThat;
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
    @DisplayName("Row batch messages")
    class Rows {

        @Test
        void shouldSerDeRows() throws Exception {
            // Given
            Schema schema = createSchemaWithKey("key", new LongType());
            List<Row> rows = List.of(
                    new Row(Map.of("key", 123L)),
                    new Row(Map.of("key", 12345L)));
            QueryWebSocketMessageSerDe serDe = QueryWebSocketMessageSerDe.forBatchSizeAndPayloadSize(3, 100, schema);

            // When
            List<String> json = toJson(serDe, "test-query", rows);
            List<QueryWebSocketMessage> found = json.stream().map(serDe::fromJson).toList();

            // Then
            assertThat(found).containsExactly(
                    QueryWebSocketMessage.rowsBatch("test-query", rows));
            Approvals.verify(json);
        }

        @Test
        void shouldSerDeTwoBatchesByBatchSize() throws Exception {
            // Given
            Schema schema = createSchemaWithKey("key", new LongType());
            List<Row> rows = List.of(
                    new Row(Map.of("key", 123L)),
                    new Row(Map.of("key", 12345L)));
            QueryWebSocketMessageSerDe serDe = QueryWebSocketMessageSerDe.forBatchSizeAndPayloadSize(1, 1000, schema);

            // When
            List<String> json = toJson(serDe, "test-query", rows);
            List<QueryWebSocketMessage> found = json.stream().map(serDe::fromJson).toList();

            // Then
            assertThat(found).containsExactly(
                    QueryWebSocketMessage.rowsBatch("test-query", List.of(new Row(Map.of("key", 123L)))),
                    QueryWebSocketMessage.rowsBatch("test-query", List.of(new Row(Map.of("key", 12345L)))));
        }

        @Test
        void shouldSerDeTwoBatchesByPayloadSize() throws Exception {
            // Given
            Schema schema = createSchemaWithKey("key", new LongType());
            List<Row> rows = List.of(
                    new Row(Map.of("key", 123L)),
                    new Row(Map.of("key", 12345L)));
            QueryWebSocketMessageSerDe serDe = QueryWebSocketMessageSerDe.forBatchSizeAndPayloadSize(null, 60, schema);

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
            Schema schema = createSchemaWithKey("key", new LongType());
            List<Row> rows = List.of(
                    new Row(Map.of("key", 123L)),
                    new Row(Map.of("key", 12345L)));
            QueryWebSocketMessageSerDe serDe = QueryWebSocketMessageSerDe.forBatchSizeAndPayloadSize(null, 10, schema);

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
            Schema schema = createSchemaWithKey("key", new LongType());
            List<Row> rows = List.of(
                    new Row(Map.of("key", 10L)),
                    new Row(Map.of("key", 20L)),
                    new Row(Map.of("key", 30L)));
            QueryWebSocketMessageSerDe serDe = QueryWebSocketMessageSerDe.forBatchSizeAndPayloadSize(2, 1000, schema);

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

        private List<String> toJson(QueryWebSocketMessageSerDe serDe, String queryId, List<Row> rows) throws Exception {
            List<String> json = new ArrayList<>();
            serDe.forEachRowBatchJson("test-query", rows.iterator(), json::add);
            return json;
        }
    }

}
