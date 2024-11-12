/*
 * Copyright 2022-2024 Crown Copyright
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

import org.junit.jupiter.api.Test;

import sleeper.core.iterator.SortedRecordIterator;
import sleeper.core.iterator.impl.AdditionIterator;
import sleeper.core.iterator.impl.AgeOffIterator;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.core.IngestResult;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.core.statestore.testutils.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;
import static sleeper.ingest.runner.testutils.IngestRecordsTestDataHelper.readIngestedRecords;

class IngestRecordsResultIT extends IngestRecordsTestBase {

    private final Schema schema = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .valueFields(new Field("value", new LongType()))
            .build();
    private final StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(schema);

    @Test
    void shouldReturnDifferentReadAndWrittenCountsWhenTableIteratorReducesCount() throws Exception {
        // Given
        List<Record> records = Arrays.asList(record("test-1", 1), record("test-1", 2), record("test-2", 3));

        // When
        IngestResult result = ingestWithTableIterator(AdditionIterator.class, records);

        // Then
        assertThat(readRecords(result)).containsExactly(record("test-1", 3), record("test-2", 3));
        assertThat(result).extracting("recordsRead", "recordsWritten")
                .containsExactly(3L, 2L);
    }

    @Test
    void shouldReturnDifferentReadAndWrittenCountsWhenTableIteratorFiltersOutAll() throws Exception {
        // Given
        List<Record> records = Arrays.asList(record("test-1", 1), record("test-1", 2), record("test-2", 3));

        // When
        IngestResult result = ingestWithTableIterator(AgeOffIterator.class, "value,0", records);

        // Then
        assertThat(readRecords(result)).isEmpty();
        assertThat(result).extracting("recordsRead", "recordsWritten")
                .containsExactly(3L, 0L);
    }

    private static Record record(String key, long value) {
        Record record = new Record();
        record.put("key", key);
        record.put("value", value);
        return record;
    }

    private IngestResult ingestWithTableIterator(
            Class<? extends SortedRecordIterator> iteratorClass, List<Record> records) throws Exception {
        return ingestWithTableIterator(iteratorClass, null, records);
    }

    private IngestResult ingestWithTableIterator(
            Class<? extends SortedRecordIterator> iteratorClass, String iteratorConfig, List<Record> records) throws Exception {
        return ingestRecordsWithTableProperties(schema, stateStore, records,
                tableProperties -> {
                    tableProperties.set(ITERATOR_CLASS_NAME, iteratorClass.getName());
                    tableProperties.set(ITERATOR_CONFIG, iteratorConfig);
                });
    }

    private List<Record> readRecords(IngestResult result) {
        return readIngestedRecords(result, schema);
    }
}
