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
package sleeper.clients.query;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.jupiter.api.BeforeEach;

import sleeper.clients.query.FakeWebSocketClient.WebSocketResponse;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QuerySerDe;
import sleeper.query.runner.websocket.QueryWebSocketMessageSerDe;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public abstract class QueryWebSocketClientTestBase {
    private static final Gson GSON = new GsonBuilder().create();
    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final Schema schema = createSchemaWithKey("key");
    protected final Field rowKey = schema.getField("key").orElseThrow();
    protected final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    protected final TableIndex tableIndex = new InMemoryTableIndex();
    protected final QuerySerDe querySerDe = new QuerySerDe(schema);
    protected final FakeWebSocketClient client = new FakeWebSocketClient();
    protected final QueryWebSocketMessageSerDe serDe = QueryWebSocketMessageSerDe.withNoBatchSize(schema);

    @BeforeEach
    void setUp() {
        instanceProperties.set(QUERY_WEBSOCKET_API_URL, "websocket-endpoint");
        tableProperties.set(TABLE_NAME, "test-table");
        tableIndex.create(tableProperties.getStatus());
    }

    public static String createdSubQueries(String queryId, String... subQueryIds) {
        return "{" +
                "\"queryId\":\"" + queryId + "\", " +
                "\"message\":\"subqueries\"," +
                "\"queryIds\":[" + Stream.of(subQueryIds).map(id -> "\"" + id + "\"").collect(Collectors.joining(",")) + "]" +
                "}";
    }

    public static String errorMessage(String queryId, String message) {
        return "{" +
                "\"queryId\":\"" + queryId + "\", " +
                "\"message\":\"error\"," +
                "\"error\":\"" + message + "\"" +
                "}";
    }

    public static String unknownMessage(String queryId) {
        return "{" +
                "\"queryId\":\"" + queryId + "\"," +
                "\"message\":\"unknown\"" +
                "}";
    }

    public static String queryResult(String queryId, Row... rows) {
        return "{" +
                "\"queryId\":\"" + queryId + "\", " +
                "\"message\":\"rows\"," +
                "\"rows\":" + GSON.toJson(List.of(rows)) +
                "}";
    }

    public static String completedQuery(String queryId, long rowCount) {
        return "{" +
                "\"queryId\":\"" + queryId + "\", " +
                "\"message\":\"completed\"," +
                "\"rowCount\":\"" + rowCount + "\"," +
                "\"locations\":[{\"type\":\"websocket-endpoint\"}]" +
                "}";
    }

    public static WebSocketResponse message(String message) {
        return client -> client.onMessage(message);
    }

    public static WebSocketResponse close(String reason) {
        return client -> client.onClose(reason);
    }

    public static WebSocketResponse error(Exception error) {
        return client -> client.onError(error);
    }

    public static String asJson(Row row) {
        return GSON.toJson(row);
    }

    protected Query exactQuery(String queryId, long value) {
        return Query.builder()
                .tableName(tableProperties.get(TABLE_NAME))
                .queryId(queryId)
                .regions(List.of(new Region(rangeFactory().createExactRange(rowKey, value))))
                .build();
    }

    protected Query rangeQuery(String queryId, long min, long max) {
        return Query.builder()
                .tableName(tableProperties.get(TABLE_NAME))
                .queryId(queryId)
                .regions(List.of(new Region(rangeFactory().createRange(rowKey, min, max))))
                .build();
    }

    protected RangeFactory rangeFactory() {
        return new RangeFactory(tableProperties.getSchema());
    }
}
