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
package sleeper.ingest.runner;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.iterator.AgeOffIterator;
import sleeper.core.iterator.SortedRowIterator;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.example.iterator.AdditionIterator;
import sleeper.ingest.core.IngestResult;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.ingest.runner.testutils.IngestRowsTestDataHelper.readIngestedRows;

class IngestRowsResultIT extends IngestRowsTestBase {

    @BeforeEach
    void setUp() {
        setSchema(Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(new Field("value", new LongType()))
                .build());
    }

    @Test
    void shouldReturnDifferentReadAndWrittenCountsWhenTableIteratorReducesCount() throws Exception {
        // Given
        List<Row> rows = Arrays.asList(row("test-1", 1), row("test-1", 2), row("test-2", 3));

        // When
        IngestResult result = ingestWithTableIterator(AdditionIterator.class, rows);

        // Then
        assertThat(readRows(result)).containsExactly(row("test-1", 3), row("test-2", 3));
        assertThat(result).extracting("rowsRead", "rowsWritten")
                .containsExactly(3L, 2L);
    }

    @Test
    void shouldReturnDifferentReadAndWrittenCountsWhenTableIteratorFiltersOutAll() throws Exception {
        // Given
        List<Row> rows = Arrays.asList(row("test-1", 1), row("test-1", 2), row("test-2", 3));

        // When
        IngestResult result = ingestWithTableIterator(AgeOffIterator.class, "value,0", rows);

        // Then
        assertThat(readRows(result)).isEmpty();
        assertThat(result).extracting("rowsRead", "rowsWritten")
                .containsExactly(3L, 0L);
    }

    private static Row row(String key, long value) {
        Row row = new Row();
        row.put("key", key);
        row.put("value", value);
        return row;
    }

    private IngestResult ingestWithTableIterator(
            Class<? extends SortedRowIterator> iteratorClass, List<Row> rows) throws Exception {
        return ingestWithTableIterator(iteratorClass, null, rows);
    }

    private IngestResult ingestWithTableIterator(
            Class<? extends SortedRowIterator> iteratorClass, String iteratorConfig, List<Row> rows) throws Exception {
        tableProperties.set(ITERATOR_CLASS_NAME, iteratorClass.getName());
        tableProperties.set(ITERATOR_CONFIG, iteratorConfig);
        StateStore stateStore = InMemoryTransactionLogStateStore.createAndInitialise(tableProperties, new InMemoryTransactionLogs());
        return ingestRows(stateStore, rows);
    }

    private List<Row> readRows(IngestResult result) {
        return readIngestedRows(result, schema);
    }
}
