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
package sleeper.query.core.rowretrieval;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.iterator.closeable.WrappedIterator;
import sleeper.core.row.Row;

import java.util.Iterator;
import java.util.function.Function;

public class TrackingIteratorFactory implements Function<Iterator<Row>, CloseableIterator<Row>> {

    private int iteratorsOpened = 0;
    private int iteratorsClosed = 0;
    private Runnable onClose = () -> {
    };

    @Override
    public CloseableIterator<Row> apply(Iterator<Row> iterator) {
        return new TrackingIterator(iterator);
    }

    public void setOnClose(Runnable onClose) {
        this.onClose = onClose;
    }

    public int getIteratorsOpened() {
        return iteratorsOpened;
    }

    public int getIteratorsClosed() {
        return iteratorsClosed;
    }

    /**
     * Wraps an iterator and tracks whether it was closed.
     */
    private class TrackingIterator extends WrappedIterator<Row> {

        TrackingIterator(Iterator<Row> iterator) {
            super(iterator);
            iteratorsOpened++;
        }

        @Override
        public void close() {
            iteratorsClosed++;
            onClose.run();
        }

    }
}
