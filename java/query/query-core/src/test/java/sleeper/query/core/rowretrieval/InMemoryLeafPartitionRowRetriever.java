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
package sleeper.query.core.rowretrieval;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.iterator.closeable.WrappedIterator;
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
 * An in memory version of a partition row retriever.
 */
public class InMemoryLeafPartitionRowRetriever implements LeafPartitionRowRetriever, LeafPartitionRowRetrieverProvider {

    private final InMemoryRowStore rowStore;

    public InMemoryLeafPartitionRowRetriever(InMemoryRowStore rowStore) {
        this.rowStore = rowStore;
    }

    @Override
    public CloseableIterator<Row> getRows(LeafPartitionQuery leafPartitionQuery, Schema dataReadSchema, TableProperties tableProperties) throws RowRetrievalException {
        return new WrappedIterator<>(getRowsOrThrow(leafPartitionQuery.getFiles())
                .filter(row -> isRowInRegion(row, leafPartitionQuery, dataReadSchema))
                .map(row -> mapToReadSchema(row, dataReadSchema))
                .iterator());
    }

    @Override
    public LeafPartitionRowRetriever getRowRetriever(TableProperties tableProperties) {
        return this;
    }

    private Stream<Row> getRowsOrThrow(List<String> files) throws RowRetrievalException {
        try {
            return rowStore.streamRows(files)
                    .collect(toUnmodifiableList())
                    .stream();
        } catch (NoSuchElementException e) {
            throw new RowRetrievalException("", e);
        }
    }

    private static boolean isRowInRegion(Row row, LeafPartitionQuery query, Schema tableSchema) {
        if (!isInRegion(row, query.getPartitionRegion(), tableSchema)) {
            return false;
        }
        for (Region region : query.getRegions()) {
            if (isInRegion(row, region, tableSchema)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isInRegion(Row row, Region region, Schema tableSchema) {
        Key key = Key.create(row.getValues(tableSchema.getRowKeyFieldNames()));
        return region.isKeyInRegion(tableSchema, key);
    }

    private static Row mapToReadSchema(Row row, Schema dataReadSchema) {
        return new Row(dataReadSchema.getAllFieldNames().stream()
                .collect(Collectors.toMap(Function.identity(), row::get)));
    }
}
