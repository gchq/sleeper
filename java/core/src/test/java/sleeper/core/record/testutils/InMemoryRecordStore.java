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
package sleeper.core.record.testutils;

import sleeper.core.record.Record;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

/**
 * Holds Sleeper records in memory, indexed as separate data files.
 */
public class InMemoryRecordStore {

    private final Map<String, List<Record>> recordsByFilename = new HashMap<>();

    /**
     * Adds a Sleeper data file to the store.
     *
     * @param filename the filename
     * @param records  the records
     */
    public void addFile(String filename, List<Record> records) {
        if (recordsByFilename.containsKey(filename)) {
            throw new IllegalArgumentException("File already exists: " + filename);
        }
        recordsByFilename.put(filename, records);
    }

    /**
     * Deletes a Sleeper data file from the store.
     *
     * @param filename the filename
     */
    public void deleteFile(String filename) {
        recordsByFilename.remove(filename);
    }

    /**
     * Deletes all Sleeper data files from the store.
     */
    public void deleteAllFiles() {
        recordsByFilename.clear();
    }

    /**
     * Retrieves all Sleeper records in the given data files.
     *
     * @param  files the filenames of the data files to retrieve
     * @return       the records
     */
    public Stream<Record> streamRecords(List<String> files) {
        return files.stream()
                .flatMap(this::getRecordsOrThrow);
    }

    private Stream<Record> getRecordsOrThrow(String filename) throws NoSuchElementException {
        if (!recordsByFilename.containsKey(filename)) {
            throw new NoSuchElementException("File not found: " + filename);
        }
        return recordsByFilename.get(filename).stream();
    }
}
