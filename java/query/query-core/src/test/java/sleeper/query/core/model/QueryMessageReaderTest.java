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

import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;

import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class QueryMessageReaderTest {

    private final Schema schema = createSchemaWithKey("key", new LongType());
    private final RangeFactory rangeFactory = new RangeFactory(schema);
    private final QuerySerDe querySerDe = new QuerySerDe(schema);
    private final InMemoryQueryJsonStore store = new InMemoryQueryJsonStore();
    private final QueryMessageReader reader = new QueryMessageReader(store, querySerDe);
    private final Query query = Query.builder()
            .queryId("test-query")
            .tableName("test-table")
            .regions(List.of(new Region(rangeFactory.createRange("key", 1L, 2L))))
            .build();

    @Test
    void shouldReadMessageHoldingQuery() {
        // Given
        String message = querySerDe.toJson(query);

        // When / Then
        assertThat(reader.readAndLoadIfStored(message).asParentQuery()).isEqualTo(query);
    }

    @Test
    void shouldReadMessagePointingToStoredQuery() {
        // Given
        store.store("queries/test-query.json", querySerDe.toJson(query));
        String message = querySerDe.toJsonPointingToStoredQuery("test-query", "queries/test-query.json");

        // When / Then
        assertThat(reader.readAndLoadIfStored(message).asParentQuery()).isEqualTo(query);
    }

    @Test
    void shouldReadMessagePointingToStoredQueryWithNoQueryId() {
        // Given
        store.store("queries/test-query.json", querySerDe.toJson(query));
        String message = "{\"queryJsonS3Key\":\"queries/test-query.json\"}";

        // When / Then
        assertThat(reader.readAndLoadIfStored(message).asParentQuery()).isEqualTo(query);
    }

    @Test
    void shouldFailWhenQueryIdDoesNotMatchStoredQuery() {
        // Given
        store.store("queries/test-query.json", querySerDe.toJson(query));
        String message = querySerDe.toJsonPointingToStoredQuery("other-query", "queries/test-query.json");

        // When / Then
        assertThatThrownBy(() -> reader.readAndLoadIfStored(message))
                .isInstanceOfSatisfying(QueryValidationException.class,
                        e -> assertThat(e.getQueryId()).contains("other-query"))
                .hasMessage("Query validation failed for query \"other-query\": " +
                        "Query ID in message does not match stored query at queries/test-query.json, found \"test-query\"");
    }

    @Test
    void shouldReportQueryIdFromMessageWhenStoredQueryCannotBeLoaded() {
        // Given
        String message = querySerDe.toJsonPointingToStoredQuery("test-query", "queries/missing.json");

        // When / Then
        assertThatThrownBy(() -> reader.readAndLoadIfStored(message))
                .isInstanceOfSatisfying(QueryValidationException.class,
                        e -> assertThat(e.getQueryId()).contains("test-query"))
                .hasMessageContaining("queries/missing.json")
                .hasCauseInstanceOf(NoSuchElementException.class);
    }

    @Test
    void shouldFailWithoutQueryIdWhenStoredQueryCannotBeLoaded() {
        // Given
        String message = "{\"queryJsonS3Key\":\"queries/missing.json\"}";

        // When / Then
        assertThatThrownBy(() -> reader.readAndLoadIfStored(message))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessageContaining("queries/missing.json");
    }

    @Test
    void shouldFailWhenPointerMessageContainsOtherQueryFields() {
        // Given
        String message = "{\"queryId\":\"test-query\",\"type\":\"Query\",\"tableName\":\"test-table\"," +
                "\"queryJsonS3Key\":\"queries/test-query.json\"}";

        // When / Then
        assertThatThrownBy(() -> reader.readAndLoadIfStored(message))
                .isInstanceOfSatisfying(QueryValidationException.class,
                        e -> assertThat(e.getQueryId()).contains("test-query"))
                .hasMessageContaining("must only contain the fields queryId and queryJsonS3Key");
        assertThat(store.getJsonByKey()).isEmpty();
    }

    @Test
    void shouldFailWhenStoredQueryPointsToAnotherStoredQuery() {
        // Given
        store.store("queries/a.json", querySerDe.toJsonPointingToStoredQuery("test-query", "queries/b.json"));
        String message = querySerDe.toJsonPointingToStoredQuery("test-query", "queries/a.json");

        // When / Then
        assertThatThrownBy(() -> reader.readAndLoadIfStored(message))
                .isInstanceOf(QueryValidationException.class)
                .hasMessageContaining("points to another stored query");
    }
}
