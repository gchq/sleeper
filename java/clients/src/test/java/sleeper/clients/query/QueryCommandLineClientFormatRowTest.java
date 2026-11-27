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
package sleeper.clients.query;

import org.junit.jupiter.api.Test;

import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryProcessingConfig;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryCommandLineClientFormatRowTest {

    private final Schema schema = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .valueFields(
                    new Field("value1", new StringType()),
                    new Field("value2", new LongType()))
            .build();

    @Test
    void shouldShowFieldsInTableSchemaOrder() {
        // Given
        Query query = queryWithConfig(QueryProcessingConfig.none());
        Row row = new Row(Map.of("value2", 2L, "key", "A", "value1", "one"));

        // When / Then
        assertThat(QueryCommandLineClient.formatRow(row, query, schema))
                .isEqualTo("Row{key=A, value1=one, value2=2}");
    }

    @Test
    void shouldLeaveOutValueFieldsThatWereNotRequested() {
        // Given
        Query query = queryWithConfig(QueryProcessingConfig.none().withRequestedValueFields(List.of("value2")));
        Row row = new Row(Map.of("key", "A", "value2", 2L));

        // When / Then
        assertThat(QueryCommandLineClient.formatRow(row, query, schema))
                .isEqualTo("Row{key=A, value2=2}");
    }

    @Test
    void shouldShowFieldComputedBySqlThatIsNotInTableSchema() {
        // Given
        Query query = queryWithConfig(QueryProcessingConfig.builder()
                .sqlQuery("SELECT count(*) FROM query_results")
                .build());
        Row row = new Row(Map.of("count(*)", 3L));

        // When / Then
        assertThat(QueryCommandLineClient.formatRow(row, query, schema))
                .isEqualTo("Row{values={count(*)=3}}");
    }

    private Query queryWithConfig(QueryProcessingConfig config) {
        return Query.builder()
                .tableName("test-table")
                .queryId("test-query")
                .regions(List.of())
                .processingConfig(config)
                .build();
    }
}
