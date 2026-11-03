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
import java.util.concurrent.atomic.AtomicLong;

/**
 * An iterator that counts the elements from a source iterator as it goes.
 *
 * @param <T> the element type
 */
public class CountingIterator<T> implements CloseableIterator<T> {

    private final CloseableIterator<T> source;
    private AtomicLong count = new AtomicLong(0);

    public CountingIterator(CloseableIterator<T> source) {
        this.source = source;
    }

    @Override
    public boolean hasNext() {
        return source.hasNext();
    }

    @Override
    public T next() {
        T row = source.next();
        count.incrementAndGet();
        return row;
    }

    @Override
    public void close() throws IOException {
        source.close();
    }

    /**
     * Retrieves the number of elements read from the source.
     *
     * @return the count
     */
    public long getCount() {
        return count.get();
    }

}
