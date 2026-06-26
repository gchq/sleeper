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
package sleeper.core.iterator.closeable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.NoSuchElementException;
import java.util.Objects;

public final class PrefetchingCloseableIterator<T> implements CloseableIterator<T> {

    private final CloseableIterator<T> inner;
    private final Deque<T> buffer;
    private boolean closed = false;

    public PrefetchingCloseableIterator(CloseableIterator<T> inner, int prefetchCount) {
        if (prefetchCount < 0) {
            throw new IllegalArgumentException("prefetchCount must be >= 0");
        }
        this.inner = Objects.requireNonNull(inner);
        this.buffer = new ArrayDeque<>(prefetchCount);

        // Eager prefetch
        for (int i = 0; i < prefetchCount && inner.hasNext(); i++) {
            buffer.addLast(inner.next());
        }
    }

    @Override
    public boolean hasNext() {
        ensureOpen();
        return !buffer.isEmpty() || inner.hasNext();
    }

    @Override
    public T next() {
        ensureOpen();
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return buffer.isEmpty() ? inner.next() : buffer.removeFirst();
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            inner.close();
            buffer.clear();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Iterator is closed");
        }
    }
}