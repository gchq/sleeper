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
package sleeper.core.util;

import org.junit.jupiter.api.Test;

import sleeper.core.iterator.AgeOffIterator;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.iterator.SortedRecordIterator;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.properties.model.CompactionMethod;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class IteratorFactoryTest {

    @Test
    public void shouldInitialiseIterator() throws IteratorCreationException {
        // Given
        ObjectFactory objectFactory = new ObjectFactory(IteratorFactoryTest.class.getClassLoader());
        IteratorFactory iteratorFactory = new IteratorFactory(objectFactory);
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new LongType()))
                .build();

        List<Record> records = List.of(
                new Record(Map.of("key", "test", "value", 10L)),
                new Record(Map.of("key", "test2", "value", 9999999999999999L)));
        CloseableIterator<Record> iterator = new WrappedIterator<>(records.iterator());

        // When
        SortedRecordIterator ageOffIterator = iteratorFactory.getIterator(AgeOffIterator.class.getName(), "value,1000", schema);
        List<Record> filtered = new ArrayList<>();
        ageOffIterator.apply(iterator).forEachRemaining(filtered::add);

        // Then
        assertThat(filtered).containsExactly(new Record(Map.of("key", "test2", "value", 9999999999999999L)));
    }

    @Test
    public void shouldCreateAggregatingIterator() throws IteratorCreationException {
        // Given
        ObjectFactory objectFactory = new ObjectFactory(IteratorFactoryTest.class.getClassLoader());
        IteratorFactory iteratorFactory = new IteratorFactory(objectFactory);
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new LongType()))
                .build();

        List<Record> records = List.of(
                new Record(Map.of("key", "test", "value", 10L)),
                new Record(Map.of("key", "test2", "value", 9999999999999999L)));
        CloseableIterator<Record> iterator = new WrappedIterator<>(records.iterator());

        // When
        SortedRecordIterator ageOffIterator = iteratorFactory.getIterator(CompactionMethod.AGGREGATION_ITERATOR_NAME, ";ageoff=value,1000,", schema);
        List<Record> filtered = new ArrayList<>();
        ageOffIterator.apply(iterator).forEachRemaining(filtered::add);

        // Then
        assertThat(filtered).containsExactly(new Record(Map.of("key", "test2", "value", 9999999999999999L)));
    }
}
