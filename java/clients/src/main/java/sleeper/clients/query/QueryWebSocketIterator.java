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
package sleeper.clients.query;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;

import java.util.Iterator;
import java.util.List;

/**
 * An interator used by QueryWebSocket. Turns an iterator into a {@link CloseableIterator}.
 *
 * @param <T> the type of elements returned by this iterator
 */
public class QueryWebSocketIterator<T> implements CloseableIterator<T>, QueryWebSocketHandler {
    private final Iterator<T> iterator;

    public QueryWebSocketIterator(Iterator<T> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public T next() {
        return iterator.next();
    }

    @Override
    public void close() {
    }

    @Override
    public void handleException(Exception e) {
        System.out.println(e);
    }

    @Override
    public void handleResults(List<Row> results) {
        System.out.println(results);
    }
}
