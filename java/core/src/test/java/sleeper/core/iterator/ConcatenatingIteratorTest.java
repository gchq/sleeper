/*
 * Copyright 2022 Crown Copyright
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import sleeper.core.record.Record;

public class ConcatenatingIteratorTest {

    private TestSupplier testSupplier1;
    private TestSupplier testSupplier2;

    @Before
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
        // Given
        ConcatenatingIterator concatenatingIterator = new ConcatenatingIterator(Lists.newArrayList(testSupplier1, testSupplier2));

        // When
        List<Record> records = new ArrayList<>();
        while(concatenatingIterator.hasNext()) {
            records.add(concatenatingIterator.next());
        }

        // Then
        List<Record> expectedRecords = Lists.newArrayList(
                new Record(Maps.toMap(Lists.newArrayList("1", "2", "3"), Integer::valueOf)),
                new Record(Maps.toMap(Lists.newArrayList("4", "5", "6"), Integer::valueOf)),
                new Record(Maps.toMap(Lists.newArrayList("7", "8", "9"), Integer::valueOf)),
                new Record(Maps.toMap(Lists.newArrayList("10", "11", "12"), Integer::valueOf)),
                new Record(Maps.toMap(Lists.newArrayList("13", "14", "15"), Integer::valueOf)),
                new Record(Maps.toMap(Lists.newArrayList("16", "17", "18"), Integer::valueOf))
        );

        assertEquals(expectedRecords, records);
    }

    @Test
    public void shouldReturnFalseForHasNextIfInitialisedWithEmptyListOfSuppliers() {
        // When
        ConcatenatingIterator concatenatingIterator = new ConcatenatingIterator(new ArrayList<>());

        // Then
        assertFalse(concatenatingIterator.hasNext());
    }

    @Test
    public void shouldReturnFalseForHasNextIfInitialisedWithNull() {
        // When
        ConcatenatingIterator concatenatingIterator = new ConcatenatingIterator(null);

        // Then
        assertFalse(concatenatingIterator.hasNext());
    }

    @Test
    public void shouldReturnFalseForHasNextIfSupplierProvidesEmptyList() {
        // Given
        TestSupplier testSupplier = new TestSupplier(new ArrayList<>());

        // When
        ConcatenatingIterator concatenatingIterator = new ConcatenatingIterator(Lists.newArrayList(testSupplier));

        // Then
        assertFalse(concatenatingIterator.hasNext());
    }

    @Test
    public void shouldReturnFalseForHasNextIfSuppliedIteratorIsNull() {
        // Given
        Supplier<CloseableIterator<Record>> nullSupplier = () -> null;

        // When
        ConcatenatingIterator concatenatingIterator = new ConcatenatingIterator(Lists.newArrayList(nullSupplier));

        // Then
        assertFalse(concatenatingIterator.hasNext());
    }

    @Test
    public void shouldReturnFalseForHasNextIfSuppliedWithMultipleEmptyIterators() {
        // Given
        TestSupplier testSupplier = new TestSupplier(new ArrayList<>());
        TestSupplier otherTestSupplier = new TestSupplier(new ArrayList<>());

        // When
        ConcatenatingIterator concatenatingIterator = new ConcatenatingIterator(Lists.newArrayList(testSupplier, otherTestSupplier));

        // Then
        assertFalse(concatenatingIterator.hasNext());
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
        assertFalse(concatenatingIterator.hasNext());
        assertTrue(testSupplier.hasSupplied());
        assertTrue(otherTestSupplier.hasSupplied());
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
        assertTrue(concatenatingIterator.hasNext());
        assertTrue(testSupplier.hasSupplied());
        assertTrue(otherTestSupplier.hasSupplied());
        assertTrue(testSupplier1.hasSupplied());

        List<Record> records = new ArrayList<>();
        while (concatenatingIterator.hasNext()) {
            records.add(concatenatingIterator.next());
        }

        assertEquals(Lists.newArrayList(
                new Record(Maps.toMap(Lists.newArrayList("1", "2", "3"), Integer::valueOf)),
                new Record(Maps.toMap(Lists.newArrayList("4", "5", "6"), Integer::valueOf)),
                new Record(Maps.toMap(Lists.newArrayList("7", "8", "9"), Integer::valueOf))
        ), records);

    }

    @Test
    public void shouldNotCallSupplierUntilItNeedsToBeAccessed() {
        // Given
        ConcatenatingIterator concatenatingIterator = new ConcatenatingIterator(Lists.newArrayList(testSupplier1, testSupplier2));

        // When
        concatenatingIterator.next();

        // Then
        assertTrue(testSupplier1.hasSupplied());
        assertFalse(testSupplier2.hasSupplied());
    }

    @Test
    public void shouldCloseIterablesAfterTheyComplete() {
        // Given
        AtomicBoolean closed = new AtomicBoolean(false);
        TestIterator testIterator = new TestIterator(() -> closed.set(true));

        // When
        ConcatenatingIterator concatenatingIterator = new ConcatenatingIterator(Lists.newArrayList((Supplier<CloseableIterator<Record>>) () -> testIterator));

        // Then
        assertFalse(concatenatingIterator.hasNext());
        assertTrue(closed.get());
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
        try {
            concatenatingIterator.hasNext();
            fail("Expected an exception");
        } catch (RuntimeException e) {
            assertEquals("Failed to close iterator", e.getMessage());
            assertNotNull(e.getCause());
        }
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
            this(() -> {});
        }

        public TestIterator(Runnable onClose) {
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
