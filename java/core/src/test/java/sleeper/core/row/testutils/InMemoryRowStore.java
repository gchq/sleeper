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
package sleeper.core.row.testutils;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.row.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

/**
 * Holds Sleeper rows in memory, indexed as separate data files.
 */
public class InMemoryRowStore {

    private final Map<String, List<Row>> rowsByFilename = new HashMap<>();

    /**
     * Adds a Sleeper data file to the store.
     *
     * @param filename the filename
     * @param rows     the rows
     */
    public void addFile(String filename, List<Row> rows) {
        if (rowsByFilename.containsKey(filename)) {
            throw new IllegalArgumentException("File already exists: " + filename);
        }
        rowsByFilename.put(filename, rows);
    }

    /**
     * Deletes a Sleeper data file from the store.
     *
     * @param filename the filename
     */
    public void deleteFile(String filename) {
        rowsByFilename.remove(filename);
    }

    /**
     * Deletes all Sleeper data files from the store.
     */
    public void deleteAllFiles() {
        rowsByFilename.clear();
    }

    /**
     * Retrieves all Sleeper rows in the given data files.
     *
     * @param  files the filenames of the data files to retrieve
     * @return       the rows
     */
    public Stream<Row> streamRows(List<String> files) {
        return files.stream()
                .flatMap(this::getRowsOrThrow);
    }

    /**
     * Creates an iterator over rows in the given data file.
     *
     * @param  filename the filename
     * @return          the rows
     */
    public CloseableIterator<Row> openFile(String filename) {
        return new WrappedIterator<>(getRowsOrThrow(filename).iterator());
    }

    private Stream<Row> getRowsOrThrow(String filename) throws NoSuchElementException {
        if (!rowsByFilename.containsKey(filename)) {
            throw new NoSuchElementException("File not found: " + filename);
        }
        return rowsByFilename.get(filename).stream();
    }
}
