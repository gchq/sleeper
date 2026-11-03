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
import java.util.function.LongConsumer;

public class ProgressTrackingIterator<T> implements CloseableIterator<T> {

    private final CloseableIterator<T> source;
    private final LongConsumer callback;
    private final long reportEveryN;
    private long count = 0;

    public ProgressTrackingIterator(CloseableIterator<T> source, LongConsumer callback, long reportEveryN) {
        this.source = source;
        this.callback = callback;
        this.reportEveryN = reportEveryN;
    }

    @Override
    public boolean hasNext() {
        return source.hasNext();
    }

    @Override
    public T next() {
        T row = source.next();
        count++;
        if (0 == count % reportEveryN) {
            callback.accept(count);
        }
        return row;
    }

    @Override
    public void close() throws IOException {
        source.close();
        if (count == 0 || count % reportEveryN != 0) {
            callback.accept(count);
        }
    }

    public long getCount() {
        return count;
    }

}
