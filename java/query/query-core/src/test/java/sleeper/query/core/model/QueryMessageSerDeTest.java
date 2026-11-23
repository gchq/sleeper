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

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class QueryMessageSerDeTest {

    private final Schema schema = createSchemaWithKey("key", new LongType());
    private final RangeFactory rangeFactory = new RangeFactory(schema);
    private final QueryMessageSerDe serDe = new QueryMessageSerDe(new QuerySerDe(schema));

    @Test
    void shouldSerDeMessageHoldingQuery() {
        // Given
        Query query = Query.builder()
                .queryId("test-query")
                .tableName("test-table")
                .regions(List.of(new Region(rangeFactory.createRange("key", 1L, 2L))))
                .build();

        // When
        String message = serDe.toJson(query);

        // Then
        assertThat(serDe.readStoredQueryKey(message)).isEmpty();
        assertThat(serDe.fromJson(message).asParentQuery()).isEqualTo(query);
    }

    @Test
    void shouldSerDeMessagePointingToStoredQuery() {
        // When
        String message = serDe.toJsonPointingToStoredQuery("queries/test-query/query.json");

        // Then
        assertThat(message).isEqualTo("{\"queryJsonS3Key\":\"queries/test-query/query.json\"}");
        assertThat(serDe.readStoredQueryKey(message)).contains("queries/test-query/query.json");
    }

    @Test
    void shouldNotReadPointerFromInvalidJson() {
        assertThat(serDe.readStoredQueryKey("{\"queryJsonS3Key\":")).isEmpty();
    }

    @Test
    void shouldNotReadPointerFromObjectWithOtherFields() {
        assertThat(serDe.readStoredQueryKey("{\"queryJsonS3Key\":\"a\",\"queryId\":\"b\"}")).isEmpty();
    }
}
