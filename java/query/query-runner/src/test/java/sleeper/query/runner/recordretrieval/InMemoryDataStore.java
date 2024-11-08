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
package sleeper.query.runner.recordretrieval;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.key.Key;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.query.core.model.LeafPartitionQuery;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

public class InMemoryDataStore implements LeafPartitionRecordRetriever {

    private final Map<String, List<Record>> recordsByFilename = new HashMap<>();

    @Override
    public CloseableIterator<Record> getRecords(LeafPartitionQuery leafPartitionQuery, Schema dataReadSchema) throws RecordRetrievalException {
        return new WrappedIterator<>(getRecordsOrRecordRetrievalException(leafPartitionQuery.getFiles())
                .filter(record -> isRecordInRegion(record, leafPartitionQuery, dataReadSchema))
                .map(record -> mapToReadSchema(record, dataReadSchema))
                .iterator());
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

    public Stream<Record> streamRecords(List<String> files) {
        return files.stream()
                .flatMap(this::getRecordsOrThrow);
    }

    private Stream<Record> getRecordsOrRecordRetrievalException(List<String> files) throws RecordRetrievalException {
        try {
            return streamRecords(files)
                    .collect(toUnmodifiableList())
                    .stream();
        } catch (NoSuchElementException e) {
            throw new RecordRetrievalException("", e);
        }
    }

    private Stream<Record> getRecordsOrThrow(String filename) throws NoSuchElementException {
        if (!recordsByFilename.containsKey(filename)) {
            throw new NoSuchElementException("File not found: " + filename);
        }
        return recordsByFilename.get(filename).stream();
    }

    private static boolean isRecordInRegion(Record record, LeafPartitionQuery query, Schema tableSchema) {
        if (!isInRegion(record, query.getPartitionRegion(), tableSchema)) {
            return false;
        }
        for (Region region : query.getRegions()) {
            if (isInRegion(record, region, tableSchema)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isInRegion(Record record, Region region, Schema tableSchema) {
        Key key = Key.create(record.getValues(tableSchema.getRowKeyFieldNames()));
        return region.isKeyInRegion(tableSchema, key);
    }

    private static Record mapToReadSchema(Record record, Schema dataReadSchema) {
        return new Record(dataReadSchema.getAllFieldNames().stream()
                .collect(Collectors.toMap(Function.identity(), record::get)));
    }
}
