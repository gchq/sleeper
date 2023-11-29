/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.query.recordretrieval;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.key.Key;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.query.model.LeafPartitionQuery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InMemoryLeafPartitionRecordRetriever implements LeafPartitionRecordRetriever {

    private final Map<String, List<Record>> recordsByFilename = new HashMap<>();

    @Override
    public CloseableIterator<Record> getRecords(LeafPartitionQuery leafPartitionQuery, Schema dataReadSchema) throws RecordRetrievalException {
        return new WrappedIterator<>(getRecordsOrThrow(leafPartitionQuery.getFiles())
                .filter(record -> isRecordInRegion(record, leafPartitionQuery, dataReadSchema))
                .map(record -> mapToReadSchema(record, dataReadSchema))
                .iterator());
    }

    public void addFile(String filename, List<Record> records) {
        recordsByFilename.put(filename, records);
    }

    private Stream<Record> getRecordsOrThrow(List<String> files) throws RecordRetrievalException {
        try {
            return files.stream()
                    .map(this::getRecordsOrThrow)
                    .collect(Collectors.toUnmodifiableList())
                    .stream().flatMap(Function.identity());
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
