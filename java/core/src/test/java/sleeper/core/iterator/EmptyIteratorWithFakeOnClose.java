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

import sleeper.core.record.Record;

import java.io.IOException;

/**
 * A test fake for an empty iterator that applies some action when it is closed.
 */
public class EmptyIteratorWithFakeOnClose implements CloseableIterator<Record> {

    private final OnClose onClose;

    public EmptyIteratorWithFakeOnClose(OnClose onClose) {
        this.onClose = onClose;
    }

    /**
     * Performs some test action when the iterator is closed.
     */
    @FunctionalInterface
    public interface OnClose {

        /**
         * Triggers the test action.
         *
         * @throws IOException if the close method should throw an exception
         */
        void close() throws IOException;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Record next() {
        return null;
    }

    @Override
    public void close() throws IOException {
        onClose.close();
    }
}
