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
package sleeper.query.core.recordretrieval;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.key.Key;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Region;
import sleeper.core.row.Row;
import sleeper.core.row.testutils.InMemoryRowStore;
import sleeper.core.schema.Schema;
import sleeper.query.core.model.LeafPartitionQuery;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * Creates an in memory version of a partition record retriever.
 */
public class InMemoryLeafPartitionRecordRetriever implements LeafPartitionRecordRetriever, LeafPartitionRecordRetrieverProvider {

    private final InMemoryRowStore recordStore;

    public InMemoryLeafPartitionRecordRetriever(InMemoryRowStore recordStore) {
        this.recordStore = recordStore;
    }

    @Override
    public CloseableIterator<Row> getRows(LeafPartitionQuery leafPartitionQuery, Schema dataReadSchema) throws RecordRetrievalException {
        return new WrappedIterator<>(getRecordsOrRecordRetrievalException(leafPartitionQuery.getFiles())
                .filter(record -> isRecordInRegion(record, leafPartitionQuery, dataReadSchema))
                .map(record -> mapToReadSchema(record, dataReadSchema))
                .iterator());
    }

    @Override
    public LeafPartitionRecordRetriever getRecordRetriever(TableProperties tableProperties) {
        return this;
    }

    private Stream<Row> getRecordsOrRecordRetrievalException(List<String> files) throws RecordRetrievalException {
        try {
            return recordStore.streamRows(files)
                    .collect(toUnmodifiableList())
                    .stream();
        } catch (NoSuchElementException e) {
            throw new RecordRetrievalException("", e);
        }
    }

    private static boolean isRecordInRegion(Row record, LeafPartitionQuery query, Schema tableSchema) {
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

    private static boolean isInRegion(Row record, Region region, Schema tableSchema) {
        Key key = Key.create(record.getValues(tableSchema.getRowKeyFieldNames()));
        return region.isKeyInRegion(tableSchema, key);
    }

    private static Row mapToReadSchema(Row record, Schema dataReadSchema) {
        return new Row(dataReadSchema.getAllFieldNames().stream()
                .collect(Collectors.toMap(Function.identity(), record::get)));
    }
}
