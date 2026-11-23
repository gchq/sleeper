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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class QueryMessageSerDeTest {

    private final Schema schema = createSchemaWithKey("key", new LongType());
    private final RangeFactory rangeFactory = new RangeFactory(schema);
    private final QuerySerDe querySerDe = new QuerySerDe(schema);
    private final QueryMessageSerDe serDe = new QueryMessageSerDe(querySerDe);
    private final Query query = Query.builder()
            .queryId("test-query")
            .tableName("test-table")
            .regions(List.of(new Region(rangeFactory.createRange("key", 1L, 2L))))
            .build();

    @Test
    void shouldSerDeMessageHoldingQuery() {
        // When
        String message = serDe.toJson(query);

        // Then
        assertThat(message).doesNotContain("queryJsonS3Key");
        assertThat(serDe.fromJson(message, noStore()).asParentQuery()).isEqualTo(query);
    }

    @Test
    void shouldSerDeMessagePointingToStoredQuery() {
        // When
        String message = serDe.toJsonPointingToStoredQuery("test-query", "queries/test-query.json");

        // Then
        assertThat(message).isEqualTo("{\"queryId\":\"test-query\",\"queryJsonS3Key\":\"queries/test-query.json\"}");
        assertThat(serDe.fromJson(message, store(Map.of("queries/test-query.json", querySerDe.toJson(query)))).asParentQuery())
                .isEqualTo(query);
    }

    @Test
    void shouldReadMessagePointingToStoredQueryWithNoQueryId() {
        // Given
        String message = "{\"queryJsonS3Key\":\"queries/test-query.json\"}";

        // When / Then
        assertThat(serDe.fromJson(message, store(Map.of("queries/test-query.json", querySerDe.toJson(query)))).asParentQuery())
                .isEqualTo(query);
    }

    @Test
    void shouldFailWhenQueryIdDoesNotMatchStoredQuery() {
        // Given
        String message = serDe.toJsonPointingToStoredQuery("other-query", "queries/test-query.json");
        Function<String, String> store = store(Map.of("queries/test-query.json", querySerDe.toJson(query)));

        // When / Then
        assertThatThrownBy(() -> serDe.fromJson(message, store))
                .isInstanceOfSatisfying(QueryValidationException.class,
                        e -> assertThat(e.getQueryId()).contains("other-query"))
                .hasMessage("Query validation failed for query \"other-query\": " +
                        "Query ID in message does not match stored query at queries/test-query.json, found \"test-query\"");
    }

    @Test
    void shouldReportQueryIdFromMessageWhenStoredQueryCannotBeLoaded() {
        // Given
        String message = serDe.toJsonPointingToStoredQuery("test-query", "queries/missing.json");

        // When / Then
        assertThatThrownBy(() -> serDe.fromJson(message, store(Map.of())))
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
        assertThatThrownBy(() -> serDe.fromJson(message, store(Map.of())))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessageContaining("queries/missing.json");
    }

    @Test
    void shouldFailWhenPointerMessageContainsOtherQueryFields() {
        // Given
        String message = "{\"queryId\":\"test-query\",\"type\":\"Query\",\"tableName\":\"test-table\"," +
                "\"queryJsonS3Key\":\"queries/test-query.json\"}";

        // When / Then
        assertThatThrownBy(() -> serDe.fromJson(message, noStore()))
                .isInstanceOfSatisfying(QueryValidationException.class,
                        e -> assertThat(e.getQueryId()).contains("test-query"))
                .hasMessageContaining("must only contain the fields queryId and queryJsonS3Key");
    }

    @Test
    void shouldFailWhenStoredQueryPointsToAnotherStoredQuery() {
        // Given
        String message = serDe.toJsonPointingToStoredQuery("test-query", "queries/a.json");
        Function<String, String> store = store(Map.of(
                "queries/a.json", serDe.toJsonPointingToStoredQuery("test-query", "queries/b.json")));

        // When / Then
        assertThatThrownBy(() -> serDe.fromJson(message, store))
                .isInstanceOf(QueryValidationException.class)
                .hasMessageContaining("points to another stored query");
    }

    private static Function<String, String> noStore() {
        return key -> {
            throw new AssertionError("Unexpected load from store: " + key);
        };
    }

    private static Function<String, String> store(Map<String, String> jsonByKey) {
        return key -> {
            String json = jsonByKey.get(key);
            if (json == null) {
                throw new NoSuchElementException("Not found: " + key);
            }
            return json;
        };
    }
}
