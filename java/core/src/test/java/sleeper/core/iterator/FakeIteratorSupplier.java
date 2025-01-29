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

import sleeper.core.record.Record;

import java.util.List;
import java.util.function.Supplier;

/**
 * A test fake to supply an iterator of records.
 */
public class FakeIteratorSupplier implements Supplier<CloseableIterator<Record>> {
    private final List<Record> records;
    private boolean hasSupplied = false;

    public FakeIteratorSupplier(List<Record> records) {
        this.records = records;
    }

    @Override
    public CloseableIterator<Record> get() {
        hasSupplied = true;
        return new WrappedIterator<>(records.iterator());
    }

    /**
     * Checks if the fake has returned an iterator.
     * 
     * @return true if an iterator was retrieved
     */
    public boolean hasSupplied() {
        return hasSupplied;
    }
}
