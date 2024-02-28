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
package sleeper.core.iterator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.record.Record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ConcatenatingIteratorTest {

    private TestSupplier testSupplier1;
    private TestSupplier testSupplier2;

    @BeforeEach
    public void resetSuppliers() {
        testSupplier1 = new TestSupplier(Lists.newArrayList(
                new Record(Maps.toMap(Lists.newArrayList("1", "2", "3"), Integer::valueOf)),
                new Record(Maps.toMap(Lists.newArrayList("4", "5", "6"), Integer::valueOf)),
                new Record(Maps.toMap(Lists.newArrayList("7", "8", "9"), Integer::valueOf))
        ));

        testSupplier2 = new TestSupplier(Lists.newArrayList(
                new Record(Maps.toMap(Lists.newArrayList("10", "11", "12"), Integer::valueOf)),
                new Record(Maps.toMap(Lists.newArrayList("13", "14", "15"), Integer::valueOf)),
                new Record(Maps.toMap(Lists.newArrayList("16", "17", "18"), Integer::valueOf))
        ));
    }

    @Test
    public void shouldReadFromEachSupplierSequentially() {
        // When
        ConcatenatingIterator concatenatingIterator = new ConcatenatingIterator(Lists.newArrayList(testSupplier1, testSupplier2));

        // Then
        assertThat(concatenatingIterator).toIterable().containsExactly(
                new Record(Maps.toMap(Lists.newArrayList("1", "2", "3"), Integer::valueOf)),
                new Record(Maps.toMap(Lists.newArrayList("4", "5", "6"), Integer::valueOf)),
                new Record(Maps.toMap(Lists.newArrayList("7", "8", "9"), Integer::valueOf)),
                new Record(Maps.toMap(Lists.newArrayList("10", "11", "12"), Integer::valueOf)),
                new Record(Maps.toMap(Lists.newArrayList("13", "14", "15"), Integer::valueOf)),
                new Record(Maps.toMap(Lists.newArrayList("16", "17", "18"), Integer::valueOf)));
    }

    @Test
    public void shouldReturnFalseForHasNextIfInitialisedWithEmptyListOfSuppliers() {
        // When
        ConcatenatingIterator concatenatingIterator = new ConcatenatingIterator(new ArrayList<>());

        // Then
        assertThat(concatenatingIterator).isExhausted();
    }

    @Test
    public void shouldReturnFalseForHasNextIfInitialisedWithNull() {
        // When
        ConcatenatingIterator concatenatingIterator = new ConcatenatingIterator(null);

        // Then
        assertThat(concatenatingIterator).isExhausted();
    }

    @Test
    public void shouldReturnFalseForHasNextIfSupplierProvidesEmptyList() {
        // Given
        TestSupplier testSupplier = new TestSupplier(new ArrayList<>());

        // When
        ConcatenatingIterator concatenatingIterator = new ConcatenatingIterator(Lists.newArrayList(testSupplier));

        // Then
        assertThat(concatenatingIterator).isExhausted();
    }

    @Test
    public void shouldReturnFalseForHasNextIfSuppliedIteratorIsNull() {
        // Given
        Supplier<CloseableIterator<Record>> nullSupplier = () -> null;

        // When
        ConcatenatingIterator concatenatingIterator = new ConcatenatingIterator(Lists.newArrayList(nullSupplier));

        // Then
        assertThat(concatenatingIterator).isExhausted();
    }

    @Test
    public void shouldReturnFalseForHasNextIfSuppliedWithMultipleEmptyIterators() {
        // Given
        TestSupplier testSupplier = new TestSupplier(new ArrayList<>());
        TestSupplier otherTestSupplier = new TestSupplier(new ArrayList<>());

        // When
        ConcatenatingIterator concatenatingIterator = new ConcatenatingIterator(Lists.newArrayList(testSupplier, otherTestSupplier));

        // Then
        assertThat(concatenatingIterator).isExhausted();
    }

    @Test
    public void shouldReturnFalseForHasNextIfSuppliedWithBothNullsAndMultipleEmptyIterators() {
        // Given
        TestSupplier testSupplier = new TestSupplier(new ArrayList<>());
        Supplier<CloseableIterator<Record>> nullSupplier = () -> null;
        TestSupplier otherTestSupplier = new TestSupplier(new ArrayList<>());

        // When
        ConcatenatingIterator concatenatingIterator = new ConcatenatingIterator(Lists.newArrayList(testSupplier, nullSupplier, null, otherTestSupplier));

        // Then
        assertThat(concatenatingIterator).isExhausted();
        assertThat(testSupplier.hasSupplied()).isTrue();
        assertThat(otherTestSupplier.hasSupplied()).isTrue();
    }

    @Test
    public void shouldReturnTrueForHasNextIfSuppliedWithBothNullsAndMultipleEmptyIteratorsAndOnePopulatedIterator() {
        // Given
        TestSupplier testSupplier = new TestSupplier(new ArrayList<>());
        Supplier<CloseableIterator<Record>> nullSupplier = () -> null;
        TestSupplier otherTestSupplier = new TestSupplier(new ArrayList<>());

        // When
        ConcatenatingIterator concatenatingIterator = new ConcatenatingIterator(Lists.newArrayList(testSupplier,
                nullSupplier, null, otherTestSupplier, testSupplier1));

        // Then
        assertThat(concatenatingIterator).hasNext();
        assertThat(testSupplier.hasSupplied()).isTrue();
        assertThat(otherTestSupplier.hasSupplied()).isTrue();
        assertThat(testSupplier1.hasSupplied()).isTrue();

        assertThat(concatenatingIterator).toIterable().containsExactly(
                new Record(Maps.toMap(Lists.newArrayList("1", "2", "3"), Integer::valueOf)),
                new Record(Maps.toMap(Lists.newArrayList("4", "5", "6"), Integer::valueOf)),
                new Record(Maps.toMap(Lists.newArrayList("7", "8", "9"), Integer::valueOf)));
    }

    @Test
    public void shouldNotCallSupplierUntilItNeedsToBeAccessed() {
        // Given
        ConcatenatingIterator concatenatingIterator = new ConcatenatingIterator(Lists.newArrayList(testSupplier1, testSupplier2));

        // When
        concatenatingIterator.next();

        // Then
        assertThat(testSupplier1.hasSupplied()).isTrue();
        assertThat(testSupplier2.hasSupplied()).isFalse();
    }

    @Test
    public void shouldCloseIterablesAfterTheyComplete() {
        // Given
        AtomicBoolean closed = new AtomicBoolean(false);
        TestIterator testIterator = new TestIterator(() -> closed.set(true));

        // When
        ConcatenatingIterator concatenatingIterator = new ConcatenatingIterator(Lists.newArrayList((Supplier<CloseableIterator<Record>>) () -> testIterator));

        // Then
        assertThat(concatenatingIterator).isExhausted();
        assertThat(closed.get()).isTrue();
    }

    @Test
    public void shouldNotThrowExceptionOnCloseIfIteratorIsNull() throws IOException {
        // When
        ConcatenatingIterator concatenatingIterator = new ConcatenatingIterator(new ArrayList<>());

        // Then
        concatenatingIterator.close();
    }

    @Test
    public void shouldThrowExceptionIfSubIteratorThrowsWhenClosing() {
        // Given
        FailingIterator failingIterator = new FailingIterator();

        // When
        ConcatenatingIterator concatenatingIterator = new ConcatenatingIterator(Lists.newArrayList((Supplier<CloseableIterator<Record>>) () -> failingIterator));

        // Then
        assertThatThrownBy(concatenatingIterator::hasNext)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed to close iterator")
                .hasCauseInstanceOf(IOException.class);
    }


    private static class FailingIterator extends TestIterator {
        @Override
        public void close() throws IOException {
            throw new IOException("AAAAHHHH");
        }
    }

    private static class TestIterator implements CloseableIterator<Record> {
        private final Runnable onClose;

        private TestIterator() {
            this(() -> {
            });
        }

        private TestIterator(Runnable onClose) {
            this.onClose = onClose;
        }

        @Override
        public void close() throws IOException {
            onClose.run();
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Record next() {
            return null;
        }
    }

    private static class TestSupplier implements Supplier<CloseableIterator<Record>> {
        private final List<Record> records;
        private boolean hasSupplied = false;

        private TestSupplier(List<Record> records) {
            this.records = records;
        }

        @Override
        public CloseableIterator<Record> get() {
            hasSupplied = true;
            return new WrappedIterator<>(records.iterator());
        }

        private boolean hasSupplied() {
            return hasSupplied;
        }
    }
}
