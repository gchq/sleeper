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
import java.util.function.Predicate;

/**
 * Filters to only include elements that meet a certain predicate.
 *
 * @param <T> the element type
 */
public class FilteringIterator<T> implements CloseableIterator<T> {

    private final CloseableIterator<T> input;
    private final Predicate<T> predicate;
    private T next;

    public FilteringIterator(CloseableIterator<T> input, Predicate<T> predicate) {
        this.input = input;
        this.predicate = predicate;
        advance();
    }

    @Override
    public boolean hasNext() {
        return null != next;
    }

    @Override
    public T next() {
        T item = next;
        if (!input.hasNext()) {
            next = null;
        }
        advance();
        return item;
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    private void advance() {
        while (input.hasNext()) {
            next = input.next();
            if (predicate.test(next)) {
                break;
            } else {
                next = null;
            }
        }
    }
}
