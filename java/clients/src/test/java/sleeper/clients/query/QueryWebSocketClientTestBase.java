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

import org.junit.jupiter.api.BeforeEach;

import sleeper.clients.query.FakeWebSocketClientAdapter.WebSocketResponse;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.testutils.InMemoryTableProperties;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.row.Row;
import sleeper.core.row.serialiser.RowJsonSerDe;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QuerySerDe;
import sleeper.query.core.output.ResultsOutputLocation;
import sleeper.query.runner.websocket.QueryWebSocketMessage;
import sleeper.query.runner.websocket.QueryWebSocketMessageSerDe;

import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public abstract class QueryWebSocketClientTestBase {
    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final Schema schema = createSchemaWithKey("key");
    protected final Field rowKey = schema.getField("key").orElseThrow();
    protected final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    protected final TableIndex tableIndex = new InMemoryTableIndex();
    protected final TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStoreReturningExactInstance(tableIndex);
    protected final TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, tablePropertiesStore);
    protected final QuerySerDe querySerDe = new QuerySerDe(schema);
    protected final FakeWebSocketClientAdapter adapter = new FakeWebSocketClientAdapter();
    protected final QueryWebSocketMessageSerDe serDe = QueryWebSocketMessageSerDe.withNoBatchSize(schema);
    protected final RowJsonSerDe rowSerDe = new RowJsonSerDe(schema);

    @BeforeEach
    void setUp() {
        instanceProperties.set(QUERY_WEBSOCKET_API_URL, "test-endpoint");
        tableProperties.set(TABLE_NAME, "test-table");
        tablePropertiesStore.createTable(tableProperties);
    }

    protected String createdSubQueries(String queryId, String... subQueryIds) {
        return serDe.toJson(QueryWebSocketMessage.queryWasSplitToSubqueries(queryId, List.of(subQueryIds)));
    }

    protected String errorMessage(String queryId, String message) {
        return serDe.toJson(QueryWebSocketMessage.queryError(queryId, message));
    }

    protected String unknownMessage(String queryId) {
        return String.format("""
                {"queryId": "%s","message": "unknown"}""", queryId);
    }

    protected String queryResult(String queryId, Row... rows) {
        return serDe.toJson(QueryWebSocketMessage.rowsBatch(queryId, List.of(rows)));
    }

    protected String completedQuery(String queryId, long rowCount) {
        return serDe.toJson(QueryWebSocketMessage.queryCompleted(queryId, rowCount,
                List.of(new ResultsOutputLocation("websocket-endpoint", "test-endpoint"))));
    }

    protected WebSocketResponse message(String message) {
        return client -> client.onMessage(message);
    }

    protected WebSocketResponse close(String reason) {
        return client -> client.onClose(reason);
    }

    protected WebSocketResponse error(Exception error) {
        return client -> client.onError(error);
    }

    protected String asJson(Row row) {
        return rowSerDe.toJson(row);
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
