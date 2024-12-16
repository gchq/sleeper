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
package sleeper.bulkexport.core.recordretrieval;

import sleeper.bulkexport.core.model.BulkExportLeafPartitionQuery;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.record.Record;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InMemoryDataStore implements LeafPartitionRecordRetriever {

    private final Map<String, List<Record>> recordsByFilename = new HashMap<>();

    @Override
    public CloseableIterator<Record> getRecords(BulkExportLeafPartitionQuery bulkExportLeafPartitionQuery)
            throws BulkExportRecordRetrievalException {

        List<Record> allRecords = bulkExportLeafPartitionQuery.getFiles().stream()
                .map(recordsByFilename::get)
                .flatMap(List::stream)
                .collect(Collectors.toList());
        return new WrappedIterator<>(allRecords.iterator());
    }

    public void addFile(String filename, List<Record> records) {
        if (recordsByFilename.containsKey(filename)) {
            throw new IllegalArgumentException("File already exists: " + filename);
        }
        recordsByFilename.put(filename, records);
    }

    public void deleteFile(String filename) {
        recordsByFilename.remove(filename);
    }

    public Collection<String> files() {
        return recordsByFilename.keySet();
    }

    public void deleteAllFiles() {
        recordsByFilename.clear();
    }
}
