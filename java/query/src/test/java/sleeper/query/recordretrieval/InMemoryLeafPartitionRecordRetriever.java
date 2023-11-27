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
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.query.model.LeafPartitionQuery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMemoryLeafPartitionRecordRetriever implements LeafPartitionRecordRetriever {

    private final Map<String, List<Record>> recordsByFilename = new HashMap<>();

    @Override
    public CloseableIterator<Record> getRecords(Schema dataReadSchema, Schema tableSchema,
                                                LeafPartitionQuery leafPartitionQuery) {
        return new WrappedIterator<>(leafPartitionQuery.getFiles().stream()
                .flatMap(filename -> recordsByFilename.get(filename).stream())
                .iterator());
    }

    public void addFile(String filename, List<Record> records) {
        recordsByFilename.put(filename, records);
    }
}
