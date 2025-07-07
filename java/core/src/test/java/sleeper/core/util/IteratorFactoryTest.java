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

import sleeper.core.iterator.AggregationFilteringIterator;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.SortedRecordIterator;
import sleeper.core.properties.model.CompactionMethod;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class IteratorFactoryTest {

    public static class StubIterator implements SortedRecordIterator {

        protected boolean isInitialised = false;

        @Override
        public CloseableIterator<Record> apply(CloseableIterator<Record> arg0) {
            throw new UnsupportedOperationException("Unimplemented method 'apply'");
        }

        @Override
        public void init(String configString, Schema schema) {
            isInitialised = true;
        }

        @Override
        public List<String> getRequiredValueFields() {
            throw new UnsupportedOperationException("Unimplemented method 'getRequiredValueFields'");
        }
    }

    @Test
    public void shouldInitialiseIterator() throws ObjectFactoryException {
        // Given
        ObjectFactory objectFactory = new ObjectFactory(IteratorFactoryTest.class.getClassLoader());
        IteratorFactory iteratorFactory = new IteratorFactory(objectFactory);

        // When
        StubIterator iterator = (StubIterator) iteratorFactory.getIterator(StubIterator.class.getName(), null, null);

        // Then
        assertThat(iterator.isInitialised).isTrue();
    }

    @Test
    public void shouldCreateAggregatingIterator() throws ObjectFactoryException {
        // Given
        ObjectFactory objectFactory = new ObjectFactory(IteratorFactoryTest.class.getClassLoader());
        IteratorFactory iteratorFactory = new IteratorFactory(objectFactory);
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("test", new IntType())).build();

        // When
        SortedRecordIterator iterator = iteratorFactory.getIterator(CompactionMethod.AGGREGATION_ITERATOR_NAME, ";,", schema);

        // Then
        assertThat(iterator).isInstanceOf(AggregationFilteringIterator.class);
    }
}
