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

import sleeper.core.row.Record;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * Iterates through a list of iterator suppliers and for each one, reads it fully.
 */
public class ConcatenatingIterator implements CloseableIterator<Record> {
    private final Iterator<Supplier<CloseableIterator<Record>>> iteratorSuppliers;
    private CloseableIterator<Record> currentIterator;

    public ConcatenatingIterator(List<Supplier<CloseableIterator<Record>>> suppliers) {
        if (suppliers != null) {
            this.iteratorSuppliers = suppliers.iterator();
            updateCurrentIterator();
        } else {
            this.iteratorSuppliers = null;
        }
    }

    private void updateCurrentIterator() {
        if (iteratorSuppliers.hasNext()) {
            Supplier<CloseableIterator<Record>> supplier = iteratorSuppliers.next();
            if (supplier != null) {
                currentIterator = supplier.get();
                return;
            }
        }
        currentIterator = null;
    }

    @Override
    public void close() throws IOException {
        if (currentIterator != null) {
            currentIterator.close();
        }
    }

    @Override
    public boolean hasNext() {
        if (currentIterator == null) {
            // If there are more iterators in the suppliers they still need to be checked
            if (iteratorSuppliers != null && iteratorSuppliers.hasNext()) {
                updateCurrentIterator();
                return hasNext();
            }
            return false;
        }
        if (!currentIterator.hasNext()) {
            try {
                currentIterator.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close iterator", e);
            }
            updateCurrentIterator();
            return hasNext();
        }
        return true;
    }

    @Override
    public Record next() {
        return currentIterator.next();
    }
}
