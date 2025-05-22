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
package sleeper.core.iterator;

import java.io.IOException;

public class LimitingIterator<T> implements CloseableIterator<T> {
    private final long limit;
    private final CloseableIterator<T> iterator;
    private long numRead = 0;

    public LimitingIterator(long limit, CloseableIterator<T> iterator) {
        this.limit = limit;
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return numRead < limit && iterator.hasNext();
    }

    @Override
    public T next() {
        T next = iterator.next();
        numRead++;
        return next;
    }

    @Override
    public void close() throws IOException {
        iterator.close();
    }

}
